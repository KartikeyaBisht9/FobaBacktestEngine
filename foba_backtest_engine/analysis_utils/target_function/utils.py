from numpy import absolute, dtype, float64, ndarray, sign, where, pi, arctan
from typing import Any, cast


def weighted_column_sum_target(column_weight_pairs, df):
    return df["trigger_direction"] * (
            sum([df[cn].values * w for (cn, w) in column_weight_pairs])
            - df["pre_midspot"].values
    )


def hkex_calculate_tick_size(price):
    if price <= 0.25:
        return 0.001
    elif price <= 0.5:
        return 0.005
    elif price <= 10.00:
        return 0.010
    elif price <= 20.00:
        return 0.020
    elif price <= 100.00:
        return 0.050
    elif price <= 200.00:
        return 0.100
    elif price <= 500.00:
        return 0.200
    elif price <= 1000.00:
        return 0.500
    elif price <= 2000.00:
        return 1.000
    elif price <= 5000.00:
        return 2.000
    elif price <= 9995.00:
        return 5.000
    else:
        return 5.000


def cap(
        values: ndarray[Any, dtype[float64]], bound: float
) -> ndarray[Any, dtype[float64]]:
    return bound * 2 / pi * arctan(values / bound * pi / 2)


def fixed_tick_bps(
        values: ndarray[Any, dtype[float64]],
        reference_price: ndarray[Any, dtype[float64]],
        tick_size: ndarray[Any, dtype[float64]],
        new_tick_range: float,
        bound: float,
) -> ndarray[Any, dtype[float64]]:
    abs_values = absolute(values)

    if new_tick_range == 0:
        # Apply soft-cap to moves > half-tick only
        return sign(values) * cast(
            "ndarray[Any, dtype[float64]]",
            where(
                abs_values < tick_size / 2,
                abs_values / reference_price,
                tick_size / 2 / reference_price
                + cap((abs_values - tick_size / 2) / reference_price, bound),
            ),
        )
    else:
        return sign(values) * cast(
            "ndarray[Any, dtype[float64]]",
            where(
                abs_values < tick_size / 2,
                abs_values * new_tick_range / tick_size,
                new_tick_range / 2
                + cap((abs_values - tick_size / 2) / reference_price, bound),
            ),
        )

def crypto_fixed_tick_bps(
        values: ndarray[Any, dtype[float64]],
        reference_price: ndarray[Any, dtype[float64]],
        tick_size: ndarray[Any, dtype[float64]],
        new_tick_range: float,
        bound: float,
) -> ndarray[Any, dtype[float64]]:
    abs_values = absolute(values)

    return sign(values) * cast(
        "ndarray[Any, dtype[float64]]",
        where(
            abs_values < tick_size / 2,
            abs_values * new_tick_range / tick_size,
            new_tick_range / 2
            + cap((abs_values - tick_size / 2) / reference_price, bound),
        ),
    )