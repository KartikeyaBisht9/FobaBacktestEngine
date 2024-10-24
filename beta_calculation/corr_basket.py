from ML_val_research.beta_calculation.helpers import decay_data, resample_data
import pandas as pd
import numpy as np
from sklearn.metrics import r2_score


"""
NOTE:   We decide how many days to feed into the corr-basket calculation
It is recommended we use 30 previousdays of data for each target code
"""

def get_decayed_log_returns(df, seconds, hl):
    #Check data has DatetimeIndex
    if type(df.index) == pd.core.indexes.datetimes.DatetimeIndex:
        pass
    else:
        df.index = pd.DatetimeIndex(df['second'])
        df['time'] = pd.to_datetime(df['time'])

    decayed_log_returns = resample_data(data = df,
                                        seconds=seconds,
                                        hl = hl,
                                        ref_index_list=[])
    return decayed_log_returns


def calculate_corr_for_target_code_residuals(residuals, df, target_code, seconds, shifted, top, threshold, use_date):
    """
    Args:
        residuals: This is the error
        df: This is the "prediction" that we have so far
        target_code:
        seconds:
        shifted:
        top:
        threshold:
        use_date:

    This is used to get the N CORRELATED symbols for our target-code as well as the beta
    """

    main_book = f'EV{target_code}'
    new_frame = pd.concat([residuals, df.drop(columns=[main_book])], axis=1)

    corrs = []
    for book in new_frame.columns:
        if book == main_book:
            corrs.append(-1)
        else:
            new_frame_dropna = new_frame[[book, main_book]].dropna()
            corrs.append(
                np.corrcoef(
                    new_frame_dropna[book].values, new_frame_dropna[main_book].values
                )[0, 1]
            )

    corr_frame = pd.DataFrame(
        list(zip(new_frame.columns, corrs)), columns=["book", "corr"]
    ).set_index("book")

    corrs_sorted = corr_frame.sort_values(by=["corr"], ascending=False)
    if corrs_sorted["corr"].iloc[top] > threshold:
        top_corrs = (
            corrs_sorted[corrs_sorted > threshold]
            .reset_index()
            .rename(columns={"book": "constituent"})
        )
    else:
        top_corrs = (
            corrs_sorted.reset_index()
            .rename(columns={"book": "constituent"})
            .iloc[:top]
        )

    top_corrs["stock_id"] = f"EV{target_code}"
    top_corrs["date"] = use_date
    top_corrs = top_corrs.query("constituent != stock_id")

    betas = []
    for stock in top_corrs["constituent"]:
        new_frame_no_na = new_frame[[stock, f"EV{target_code}"]].dropna()
        beta = np.linalg.lstsq(
            new_frame_no_na[stock].values.reshape(-1, 1),
            new_frame_no_na[f"EV{target_code}"],
            rcond=None,
        )[0][0]
        betas.append(beta)
    top_corrs["beta"] = betas
    top_corrs.assign(resample=seconds, top=top, threshold=threshold, shifted=shifted)

    del corr_frame
    del corrs_sorted
    del betas

    return top_corrs

def residual_corrs(combined, max_iters, code, lr, seconds, shifted, top, threshold, date):
    """

    Args:
        combined: This is the dataframe of stock returns
        max_iters: This is the number of stocks we use to make the corr-basket
        code: This is the target code
        lr: This is the learning rate (defaulted to 0.50)
        seconds: This is the resample time period
        shifted:
        date: This is the date we are running for
        top: This is the number of symbols to get the correlation for
        threshold: This is the minimum correlation needed

    Returns:

    """
    if f"EV{code}" not in combined.columns:
        return pd.DataFrame()
    og_data = combined[f"EV{code}"].copy()
    residuals = combined[f"EV{code}"].copy()
    cum_pred = np.zeros_like(og_data)
    r2 = 0
    single_corrs = []

    for it in range(max_iters):
        corrs = calculate_corr_for_target_code_residuals(
            residuals, combined, code, seconds, shifted, 1, 1, date
        )
        current_stock = corrs["constituent"][0]
        current_beta = corrs["beta"][0]
        current_corr = corrs["corr"][0]
        residuals -= lr * combined[current_stock] * current_beta
        cum_pred += lr * combined[current_stock] * current_beta
        mask = ~og_data.isna() & ~cum_pred.isna()
        cum_values = cum_pred.values[mask.values]
        og_values = og_data.values[mask.values]
        r2_new = r2_score(og_values.reshape(-1,1), cum_values.reshape(-1,1))
        if r2_new < r2:
            if it != 0:
                break
        corrs["r2_gain"] = r2_new - r2
        corrs = corrs.assign(
            max_iters=max_iters, lr=lr, iteration=it, r2_prior=r2, r2_after=r2_new
        )
        r2 = float(r2_new)
        single_corrs.append(corrs)

    corr_frame = pd.concat(single_corrs)
    corr_frame["iters"] = it

    del og_data
    del residuals
    del cum_pred
    del single_corrs

    return corr_frame


"""
NOTE:   The RWS-SECOND dataframe that is passed in must be such that the FINAL DATE is "yesterday" 
        The number of dates we feed in is determined by back-testing:  default of 30 is recommended
"""


def all_corr_baskets_for_one_day(codes, rws_seconds_data, date,  max_iters, lr, hl, seconds, shifted, top, threshold):
    code_result = {}
    decayed_log_returns = get_decayed_log_returns(rws_seconds_data, seconds, hl)

    for code in codes:
        print(f'CorrBsk Calculations for {code}')
        code_result[code] = residual_corrs(decayed_log_returns, max_iters, code, lr, seconds, shifted, top, threshold, date)

    return code_result, date

def all_corr_baskets_for_many_days(codes, rws_seconds_data, dates, max_iters, lr, hl, seconds, shifted, top, threshold):
    full_result = {}
    for date in dates:
        full_result[date] = all_corr_baskets_for_one_day(codes, rws_seconds_data, date,  max_iters, lr, hl, seconds, shifted, top, threshold)
    return full_result


