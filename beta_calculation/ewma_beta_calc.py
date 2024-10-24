from ML_val_research.beta_calculation.helpers import decay_data, resample_data, index_corrs
import pandas as pd
import numpy as np


def ema_betas(beta_frame, hl, dates):
    complete_list = []
    for (stock, index), data in beta_frame.groupby(["stock", "index"]):
        new_data = data.sort_index().reset_index(drop=True)
        new_data['beta']=new_data.beta.ewm(halflife=hl).mean()
        new_data['opp_beta']=new_data.opp_beta.ewm(halflife=hl).mean()
        dummy = pd.DataFrame(index = dates)
        combined = pd.concat([new_data.set_index("date"),dummy], axis=1).sort_index().fillna(method='ffill').reset_index().rename(columns={"level_0":"date"})
        complete_list.append(combined)

    return pd.concat(complete_list)

def get_ewma_betas(rws_seconds_input, dates, codes, seconds, ref_index_list, decay_hl, ewma_hl):
    """

    This is GENERAL PURPOSE and can be used for any symbol-index pair
    The main things to be careful about:
        Data format = make sure rws_seconds has the correct format
        dates, codes, seconds, ref_half_life, ewma_half_life = these should be sensible
        ref_index_list = this should be a list of the "reference/lead" indices you want to use

    Args:
        rws_seconds_input (dataframe)
        dates (list of dt.datetime.date)
        codes (list of strings)
        seconds (float or int)
        ref_index_list (list of strings)
        decay_hl (float or int)
        ewma_hh (float or int)

    Returns:
        dict: corr_frame, ema_frame

    """
    results = {}
    all_corrs = []

    #######
    # Gets the Beta value PER DAY for each symbol-index pair
    #######

    for date in dates:
        #Gets the combined df for ALL symbols's RWS
        rws_seconds = rws_seconds_input.query("date == @date")
        rws_seconds["time"] = rws_seconds['second'].dt.time
        rws_seconds["date"] = rws_seconds['second'].dt.date
        rws_seconds = rws_seconds.set_index("second").sort_index()

        #Resamples the above data w/ freq = seconds AND determines the decayed-log-returns of RWS
        decayed_log_returns = resample_data(rws_seconds, seconds, ref_index_list, decay_hl)

        code_list = []
        for code in codes:
            #For all PAIR of index-symbol this determines the BETA and OPP BETA for that day
            code_corrs = index_corrs(
                decayed_log_returns, code, seconds, date, ref_index_list
            )
            code_list.append(code_corrs)

        day_corrs = pd.concat(code_list)
        day_corrs['date'] = date
        all_corrs.append(day_corrs)

        del rws_seconds, decayed_log_returns, code_list, day_corrs

    ## Concats the betas into a SINGLE DF
    all_corrs_frame = pd.concat(all_corrs)

    ## Calculates an Exponentially Weighted Average of the BETAs -- the most recent BETA is now used for trading
    ema_frame = ema_betas(all_corrs_frame, hl=ewma_hl, dates=dates)

    results['corr_frame'] = all_corrs_frame
    results['ema_frame'] = ema_frame
    return results
