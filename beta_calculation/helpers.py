import numpy as np
import pandas as pd
from ML_val_research.utils.decayed_sum_module import decayed_sum
import datetime as datetime


def decay_data(data, hl, ref_index_list, reverse=False):
    try:
        data = data.to_frame()
    except:
        pass

    data["nanos"] = data.index.astype("int64")  # Assumes datetime64[ns] type
    decayed_returns = []
    cols = []

    try:
        for col in data.columns:
            if ("EV" in col) or (col in ref_index_list):
                decayed_returns.append(
                    pd.Series(
                        decayed_sum(
                            data["nanos"].values.astype(int),
                            data[col].fillna(0).values,
                            np.ones_like(data.nanos.values.astype(int))
                            * np.array([hl]).reshape((-1, 1))[0],
                            reverse=reverse,
                        ),
                        index=data.index,
                        name=col,
                    )
                )
            if len(decayed_returns) == 0:
                decayed_returns_frame = pd.DataFrame()
            else:
                decayed_returns_frame = pd.concat(decayed_returns, axis=1)
    except:
        decayed_returns.append()

    return decayed_returns_frame


def resample_data(data, seconds,ref_index_list, hl=None):
    if type(data.index) == pd.core.indexes.datetimes.DatetimeIndex:
        pass
    else:
        data.index = pd.DatetimeIndex(data['second'])

    # start = datetime.time(hour=1, minute=30, second=0)
    # end = datetime.time(hour=8, minute=0, second=0)
    stocks = {col: "last" for col in data.columns if ("EV" in col) or (col in ref_index_list)} #added index
    other = {
        "time": "last",
        "date": "last",
        "eod_flag": "sum",
    }
    resampled = data.resample(f"{seconds}s").agg({**stocks, **other})
    # resampled = resampled.query("(time >= @start and time < @end)")
    resampled.loc[(resampled.eod_flag == 1)] = np.nan
    resampled = resampled.drop(columns=["time", "date", "eod_flag"])
    resampled = np.log2(resampled) - np.log2(resampled.shift(1))

    if hl is not None:
        resampled_decayed = decay_data(resampled, hl, ref_index_list)
        return resampled_decayed

    return resampled


def index_corrs(combined, code, seconds, date, ref_index_list, ema=True):
    if code not in combined.columns:
        return pd.DataFrame()
    main_book = code

    stock_info = []
    if ema:
        hti_base=1
        for index in ref_index_list:
            index_dict = {}
            index_dict["stock"] = main_book
            index_dict["index"] = index
            combined_no_na = combined[[main_book,index]].dropna()
            beta = np.linalg.lstsq(
                combined_no_na[index].values.reshape(-1, 1),
                combined_no_na[main_book].values.reshape(-1, 1),
                rcond=None,
            )[0][0][0]
            opp_beta = np.linalg.lstsq(
                combined_no_na[main_book].values.reshape(-1, 1),
                combined_no_na[index].values.reshape(-1, 1),
                rcond=None,
            )[0][0][0]
            index_dict["beta"] = beta
            index_dict["opp_beta"] = opp_beta
            index_dict["date"] = date
            stock_info.append(index_dict)
            hti_base=beta
            hti_opp=opp_beta

    else:
        for index in ref_index_list:
            index_dict = {}
            index_dict["stock"] = main_book
            index_dict["index"] = index
            combined_no_na = combined[[main_book,index]].dropna()
            beta = np.linalg.lstsq(
                combined_no_na[index].values.reshape(-1, 1),
                combined_no_na[main_book].values.reshape(-1, 1),
                rcond=None,
            )[0][0][0]
            opp_beta = np.linalg.lstsq(
                combined_no_na[main_book].values.reshape(-1, 1),
                combined_no_na[index].values.reshape(-1, 1),
                rcond=None,
            )[0][0][0]
            index_dict["beta"] = beta
            index_dict["opp_beta"] = opp_beta
            index_dict["date"] = date
            stock_info.append(index_dict)

    return pd.DataFrame(stock_info)

