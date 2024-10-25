import logging
from typing import Any, Literal, cast

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from numpy import (
    average,
    bincount,
    cumsum,
    dtype,
    float64,
    histogram_bin_edges,
    isinf,
    isnan,
    maximum,
    minimum,
    ndarray,
    ones_like,
    searchsorted,
)
from pandas import DataFrame

LOGGER = logging.getLogger(__name__)


def _resolve_x_range(
    xs: ndarray[Any, dtype[float64]],
    x_mean: float,
    x_std: float,
    *,
    x_range: tuple[float, float] or float or None = (-2.0, 2.0),
    x_range_units: Literal["STD"] or None = "STD",
) -> tuple[float, float]:
    if x_range is None:
        return (xs.min(), xs.max())

    if isinstance(x_range, (int, float)):
        x_range = (-x_range, x_range)

    if x_range_units is None:
        return x_range

    if x_range_units == "STD":
        return (
            max(x_mean + x_range[0] * x_std, xs.min()),
            min(x_mean + x_range[1] * x_std, xs.max()),
        )
    raise ValueError(f"Could not resolve {x_range_units}")


def f_plot(
    data: DataFrame,
    *,
    x: str,
    y: str,
    weight: str or None = None,
    x_range: tuple[float, float] or float | None = (-2.0, 2.0),
    x_range_units: Literal["STD"] or None = "STD",
    num_bins: int = 128,
    std: bool = False,
    sided_means: bool = False,
    mean_vline: bool = True,
    std_vlines: bool = False,
    hist: bool = True,
    color: Any = None,
    include_context_stat: bool = False,
    title: bool = True,
    legend: bool = True,
    ax: Axes or None = None,
) -> Axes:
    xs = data[x].to_numpy()
    ys = data[y].to_numpy()
    weights = data[weight].to_numpy() if weight is not None else ones_like(xs)

    nans = isnan(xs) | isnan(ys) | isnan(weights)
    infs = isinf(xs) | isinf(ys) | isinf(weights)

    nan_count = nans.sum()
    inf_count = infs.sum()
    if nan_count or inf_count:
        LOGGER.warning("%s nans, %s infs dropped", nan_count, inf_count)

    valid = ~nans & ~infs
    xs, ys, weights = xs[valid], ys[valid], weights[valid]

    x_mean = cast(float, average(xs, weights=weights))
    x_std = cast(float, average((xs - x_mean) ** 2, weights=weights) ** (1 / 2))

    x_range = _resolve_x_range(
        xs, x_mean, x_std, x_range=x_range, x_range_units=x_range_units
    )

    edges = histogram_bin_edges(xs, bins=num_bins, range=x_range)
    bins = minimum(maximum(searchsorted(edges, xs) - 1, 0), num_bins - 1)
    counts = bincount(bins, weights, num_bins)
    sums = bincount(bins, weights * ys, num_bins)
    means = sums / counts
    values = [("means", means)]
    if std:
        stds = (
            bincount(bins, (weights * (ys - means[bins])) ** 2, num_bins)
            / ((xs.size - 1) * counts / xs.size)
        ) ** (1 / 2)
        values += [("+0.2 std", means + 0.2 * stds), ("-0.2 std", means - 0.2 * stds)]
    if sided_means:
        l_means = cumsum(sums) / cumsum(counts)
        r_means = cumsum(sums[::-1])[::-1] / cumsum(counts[::-1])[::-1]

        values += [
            ("l-means", l_means),
            ("gain", r_means - l_means),
            ("r-means", r_means),
        ]

    if ax is None:
        ax = cast(Axes, plt.gca())

    for label, value in values:
        ax.hlines(value, edges[:-1], edges[1:], label=label)

    if hist:
        hist_ax = ax.twinx()

        hist_ax.stairs(counts, edges, fill=True, alpha=0.25)

    if mean_vline:
        ax.axvline(x=x_mean)

    if std_vlines:
        for i in range(3):
            if x_mean - i * x_std >= x_range[0]:
                ax.axvline(x=x_mean - i * x_std)

            if x_mean + i * x_std <= x_range[1]:
                ax.axvline(x=x_mean + i * x_std)

    def abs_mean(x):
        return np.mean(np.abs(x))

    def get_stat(tss, target, feature, nbins=50):
        return np.abs(
            round(
                tss[[target, feature]]
                .groupby(pd.qcut(tss[feature], nbins, duplicates="drop"))
                .agg(
                    {
                        target: np.std,  # can also do abs_mean here.
                        feature: abs_mean,
                    }
                )
                .corr(method="spearman")[feature]
                .iloc[0],
                2,
            )
        )

    context_stat = get_stat(data, y, x)

    if title:
        if include_context_stat:
            ax.set(title=f"[context_stat: {context_stat}] {x} vs {y}")
        else:
            ax.set(title=f"{x} vs {y}")

    if legend:
        ax.legend()

    return ax
