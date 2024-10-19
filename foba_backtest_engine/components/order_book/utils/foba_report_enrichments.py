from foba_backtest_engine.components.order_book.utils.enums import EventType, Side
from collections import defaultdict
import pandas as pd
import numpy as np

def quoting_priority_enrichments(foba):
    """
    These enrichments allow us to configure quoter execution at a very granulary level 
    """
    foba['book_width_at_event'] = foba['asks_0_price_'] - foba['bids_0_price_']
    foba['book_width_at_join'] = foba['asks_0_price_at_join_'] - foba['bids_0_price_at_join_']
    foba['ticks_wide_at_event'] = (foba['book_width_at_event'] / foba['tick_size']).round(0)
    foba['ticks_wide_at_join'] = (foba['book_width_at_join'] / foba['tick_size']).round(0)

    foba['ticks_wide_at_event_bucket'] = np.where(foba.ticks_wide_at_event == 1, '1', np.where(foba.ticks_wide_at_event == 2, '2', np.where(foba.ticks_wide_at_event > 2, '3+', '0')))
    foba['ticks_wide_at_join_bucket'] = np.where(foba.ticks_wide_at_join == 1, '1', np.where(foba.ticks_wide_at_join == 2, '2', np.where(foba.ticks_wide_at_join > 2, '3+', '0')))

    foba['volume_behind_factor'] = foba['volume_behind_at_event'] / foba['join_volume']
    foba['volume_behind_bbov_factor'] = foba['volume_behind_at_event'] / foba['raw_bbov']

    foba['volume_ahead_factor'] = foba['volume_ahead_at_join'] / foba['join_volume']
    foba['volume_ahead_bbov_factor'] = foba['volume_ahead_at_join'] / foba['raw_bbov']

    return foba

def time_enrichments(foba):
    last_time_date = foba.event_received_datetime.max().date().strftime('%Y-%m-%d')
    foba['event_received_time'] = pd.to_datetime(last_time_date + ' ' + foba.event_received_string)
    foba['join_received_time'] = pd.to_datetime(last_time_date + ' ' + foba.join_received_string)
    foba['event_time_from_start_of_day_second'] = foba.event_time_from_start_of_day_seconds.astype(int)
    foba['event_time_from_start_of_day_hours'] = foba['event_time_from_start_of_day_seconds'] / 3600
    foba['hour'] = foba['event_time_from_start_of_day_second'] // 3600
    foba['minute'] = (foba['event_time_from_start_of_day_second'] - foba['hour'] * 3600) // 60
    return foba

def enrich_level_metrics(foba):
    foba['remaining_level_volume'] = foba['level_volume_at_event'] - foba['aggressor_volume']
    foba['whole_level_trades'] = foba['remaining_level_volume']<=0
    foba['level_clearing'] = foba['remaining_volume']==0
    foba['aggressor_turnover'] = foba['aggressor_volume'] * foba['event_price']
    foba['aggressor_level_fraction'] = foba['aggressor_volume'] / foba['level_volume_at_event']
    foba['level_fraction_at_join'] = foba['join_volume'] / (foba['join_volume'] + foba['volume_ahead_at_join'])
    foba['level_age_join_in_s'] = (foba['join_exchange_timestamp'] - foba['level_driver_received']) / 1e9
    foba['optiver_level_age_join_in_s'] = (foba['join_driver_received'] - foba['level_driver_received']) / 1e9
    foba['level_age_event_in_s'] = (foba['event_driver_received'] - foba['level_driver_received']) / 1e9
    return foba

def enrich_counterparty(foba):
    competitor_map = {
        'Optiver Trading Hong Kong Limited': 'Optiver',
        'Citadel Securities (Hong Kong) Limited': 'Citadel',
        'Eclipse Options (HK) Limited': 'Eclipse',
        'Barclays Capital Asia Limited': 'Barclays',
        'ABN AMRO Clearing Hong Kong Limited': 'ABN',
        'Yue Kun Research Limited': 'Yue Kun',
        'IMC Asia Pacific Limited': 'IMC',
        'Jane Street Asia Pacific Limited' : 'Jane Street',
        'Jump Trading Hong Kong Limited':'Jump Trading',
        'VivCourt Trading HK Limited' : 'VivCourt',
        'Barclays Hit':'Barclays Hit'
    }
    competitor_map = defaultdict(lambda: 'MKT', competitor_map)

    competitor_id_map = {
        5342: 'JP',
        639: 'UBS'
    }

    """
    TODO: We need to constantly screen for good hitting/quoting counterparties
    - Look out for XTX (this might be the ABN group we see)
    - Look out for Ren tech
    """

    foba['broker_name'] = foba['broker_name'].replace(np.nan, '')
    foba['foreign_counterparty']= foba['foreign_counterparty'].replace(np.nan, '')
    foba['competitor_from_ids'] = foba['broker_number'].map(competitor_id_map)
    foba['competitor'] = foba['broker_name'].map(competitor_map)
    foba['competitor'] = np.where(foba['competitor_from_ids'].isna(), foba['competitor'],
                                        foba['competitor_from_ids'])
    
    foba['foreign_counterparty'] = np.where(foba.optiver_hit, 'Optiver Trading Hong Kong Limited', foba.foreign_counterparty)
    foba['aggressive_competitor'] = foba['foreign_counterparty'].map(competitor_map)
    foba['competitor'] = np.where(foba.optiver_order, 'Optiver', foba.competitor)
    foba['competitor'] = np.where(np.logical_and(~foba.optiver_order, foba.competitor == 'Optiver'), 'MKT', foba.competitor)
    foba['competitor'] = np.where(np.logical_and(foba.optiver_aggressor, foba.competitor == 'Optiver'), 'MKT', foba.competitor)
    foba['competitor'] = np.where(np.logical_and(foba.optiver_hit, foba.competitor == 'Optiver'), 'MKT', foba.competitor)
    foba['broker_optiver'] = foba['competitor'] == 'Optiver'
    foba['aggressive_competitor'] = np.where(np.logical_and(~foba.optiver_hit, foba.aggressive_competitor == 'Optiver'), 'MKT',
                                             foba.aggressive_competitor)
    foba['aggressive_competitor'] = np.where(np.logical_and(foba.optiver_hit, foba.aggressive_competitor != 'Optiver'), 'Optiver',
                                             foba.aggressive_competitor)
    foba['aggressive_competitor'] = np.where(foba['foreign_counterparty_number'].isin([4487, 4488, 2077, 2078, 2079]), 'Barclays Hit', foba['aggressive_competitor'])
    foba['foreign_counterparty'] = np.where(foba['foreign_counterparty_number'].isin([4487, 4488, 2077, 2078, 2079]), 'Barclays Hit', foba['foreign_counterparty'])

    return foba

def enrich_liquidity(foba):
    foba['smooth_bbov_cash'] = foba['smooth_bbov'] * foba['event_price']
    foba['bbov_cash'] = foba['raw_bbov'] * foba['event_price']
    foba['bbov_fraction_at_join'] = foba['join_volume'] / foba['raw_bbov']
    foba['bbov_fraction_at_event'] = foba['event_volume'] / foba['raw_bbov']
    foba['aggressor_bbov_fraction_at_event'] = foba['aggressor_volume'] / foba['raw_bbov']
    return foba

def credit_and_pnl_enrichment(foba):
    """
    For val = optiverXgb | rws
        - we want to know:  
                a) max_lifetime_credit & max_lifetime_credit_bps/ticks ++ (max_credit_time_since_join) ++ (max_credit_time_to_trade)
                b) credit_at_event ... bps/ticks
                c) min_liftetime_credit & min_liftetime_credit_bps/ticks ++ (min_credit_time_since_join) ++ (min_credit_time_since_join)
    """
    return foba

def generate_field_name(type, val_type, metric):
    if metric == "":
        if type in ["min", "max"]:
            return f"{type}_lifetime_{val_type}_credit"
        else:
            return f"credit_{val_type}_at_event"
    else:
        if type in ["min", "max"]:
            return f"{type}_lifetime_{val_type}_credit_{metric}"
        else:
            return f"credit_{val_type}_at_event_{metric}"
        
    

def enrich_buckets(foba):
    """
    Time buckets
    """
    time_edges = [9.5 * 60 * 60, 10 * 60 * 60, 13 * 60 * 60, 15 * 60 * 60, 15.5 * 60 * 60, 15.75 * 60 * 60,
                  (15 + 55 / 60) * 60 * 60, (15 + 58 / 60) * 60 * 60, (15 + 59 / 60) * 60 * 60, 16.1 * 60 * 60]
    time_labels = ['09:30', '10:00', '13:00', '15:00', '15:30', '15:45', '15:55', '15:58', '15:59']
    foba['join_time_bucket'] = pd.cut(foba['join_time_from_start_of_day_seconds'], time_edges, labels=time_labels,
                                      include_lowest=True, right=False).astype('str')
    foba['trade_time_bucket'] = pd.cut(foba['event_time_from_start_of_day_seconds'], time_edges, labels=time_labels,
                                       include_lowest=True, right=False).astype('str')

    time_edges = [9 * 60 * 60, (9 + 31 / 60) * 60 * 60, 10 * 60 * 60, 12 * 60 * 60, (15 + 15 / 60) * 60 * 60,
                  (15 + 50 / 60) * 60 * 60, 16.1 * 60 * 60]
    time_labels = ['OPENING', 'FIRST_30', 'MORNING', 'AFTERNOON', 'LAST_45', 'LAST_10']
    foba['join_time_category'] = pd.cut(foba['join_time_from_start_of_day_seconds'], time_edges, labels=time_labels,
                                        include_lowest=True, right=False).astype('str')
    foba['trade_time_category'] = pd.cut(foba['event_time_from_start_of_day_seconds'], time_edges, labels=time_labels,
                                         include_lowest=True, right=False).astype('str')

    time_edges = [9.5 * 60 * 60, (9 + 31 / 60) * 60 * 60, 10 * 60 * 60, 12 * 60 * 60, (15 + 20 / 60) * 60 * 60,
                  (15 + 45 / 60) * 60 * 60, (15 + 58 / 60) * 60 * 60, 16.1 * 60 * 60]
    time_labels_granular = ['OPEN_1', 'OPEN_30', 'MORNING', 'AFTERNOON', 'LAST_40', 'LAST_15', 'LAST_2']
    foba['join_time_category_granular'] = pd.cut(foba['join_time_from_start_of_day_seconds'], time_edges,
                                                 labels=time_labels_granular, include_lowest=True, right=False).astype(
        'str')
    foba['trade_time_category_granular'] = pd.cut(foba['event_time_from_start_of_day_seconds'], time_edges,
                                                  labels=time_labels_granular, include_lowest=True, right=False).astype(
        'str')
    
    """
    Priority Offset Factors
    """
    
    foba['volume_behind_factor_bucket'] = np.where(foba['volume_behind_factor'] < 0.5, '0.0-0.5', np.where(foba['volume_behind_factor'] < 2, '0.5-2.0', np.where(foba['volume_behind_factor'] < 5, '2.0-5.0', '5.0+')))
    foba['volume_behind_bbov_factor_bucket'] = np.where(foba['volume_behind_bbov_factor'] < 0.1, '0.0-0.1', np.where(
    foba['volume_behind_bbov_factor'] < 0.25, '0.1-0.25', np.where(foba['volume_behind_bbov_factor'] < 0.5, '0.25-0.5',np.where(foba['volume_behind_bbov_factor'] < 1, '0.5-1','1.0+'))))


    foba['volume_ahead_factor_bucket'] = np.where(foba['volume_ahead_factor'] < 0.5, '0.0-0.5', np.where(foba['volume_ahead_factor'] < 2, '0.5-2.0', np.where(foba['volume_ahead_factor'] < 5, '2.0-5.0', '5.0+')))
    foba['volume_ahead_bbov_factor_bucket'] = np.where(foba['volume_ahead_bbov_factor'] < 0.1, '0.0-0.1', np.where(
    foba['volume_ahead_bbov_factor'] < 0.25, '0.1-0.25', np.where(foba['volume_ahead_bbov_factor'] < 0.5, '0.25-0.5',np.where(foba['volume_ahead_bbov_factor'] < 1, '0.5-1','1.0+'))))

    """
    Event BBOV fraction buckets
    """

    edges = [0, 0.25, 0.5, 0.75, 0.99, 1.1]
    labels = ['[0.0, 0.25]', '[0.25, 0.5]', '[0.5, 0.75]', '[0.75, 0.99]', '[0.99, 1]']
    foba['aggressor_level_fraction_bucket'] = pd.cut(foba['aggressor_level_fraction'], edges, labels=labels,
                                                     include_lowest=True, right=False).astype('str')

    edges = [0, 10, 15, 20, 1000]
    labels = ['[0, 10]', '[10, 15]', '[15, 20]', '[>20]']
    foba['tick_size_bps_bucket'] = pd.cut(foba['tick_size_bps'], edges, labels=labels, include_lowest=True,
                                          right=False).astype('str')

    edges = [0, 0.05, 0.10, 0.20, 0.50, 1.0, 1000.0]
    labels = ['[0.0, 0.05]', '[0.05, 0.10]', '[0.10, 0.20]', '[0.20, 0.50]', '[0.50, 1.0]', '[>1.00]']
    foba['level_fraction_at_join_bucket'] = pd.cut(foba['level_fraction_at_join'], edges, labels=labels,
                                                   include_lowest=True, right=False).astype('str')
    
    edges = [0, 0.05, 0.10, 0.20, 0.50, 1.0, 1000.0]
    labels = ['[0.0, 0.05]', '[0.05, 0.10]', '[0.10, 0.20]', '[0.20, 0.50]', '[0.50, 1.0]', '[>1.00]']
    foba['bbov_fraction_at_join_bucket'] = pd.cut(foba['bbov_fraction_at_join'], edges, labels=labels,
                                                  include_lowest=True, right=False).astype('str')

    edges = [0, 0.05, 0.10, 0.20, 0.50, 1.0, 1000.0]
    labels = ['[0.0, 0.05]', '[0.05, 0.10]', '[0.10, 0.20]', '[0.20, 0.50]', '[0.50, 1.0]', '[>1.00]']
    foba['aggressor_bbov_fraction_at_event_bucket'] = pd.cut(foba['aggressor_bbov_fraction_at_event'], edges,
                                                             labels=labels, include_lowest=True, right=False).astype('str')
    
    edges = [0, 0.05, 0.10, 0.20, 0.50, 1.0, 1000.0]
    labels = ['[0.0, 0.05]', '[0.05, 0.10]', '[0.10, 0.20]', '[0.20, 0.50]', '[0.50, 1.0]', '[>1.00]']
    foba['level_fraction_at_join_bucket'] = pd.cut(foba['level_fraction_at_join'], edges, labels=labels,
                                                   include_lowest=True, right=False).astype('str')
    

    edges = [-10000, 0, 1, 2, 3, 4, 5, 10000]
    labels = ['< 0', '[0, 1]', '[1, 2]', '[2, 3]', '[3, 4]', '[4, 5]', '> 5']

    types = ["", "min", "max"]
    val_types = ["midspot", "rws", "xgb"]
    metrics = ["", "bps", "ticks"]

    for type in types:
        for val_type in val_types:
            for metric in metrics:
                field = generate_field_name(type, val_type, metric)
                foba[field + "_bucket"] = pd.cut(foba[field], edges, labels=labels, include_lowest=True, right=False).astype('str')
    
    return foba

def pnl_enrichment(foba, pnl_slippages=[5,15,30,60,120,240,300,600,900,1800,3600,7200]):
    foba = foba.sort_values("createdNanos_").reset_index(drop = True)
    end_prices = foba.groupby(['date', 'product_symbol'])[['event_price']].last().to_dict()['event_price']

    def get_end_price(date, symbol):
        return end_prices[(date, symbol)]

    foba['currency_rate'] = foba['turnover'] / (foba['event_volume'] * foba['event_price'])
    foba['eod_price'] = foba[['date', 'product_symbol']].apply(lambda x: get_end_price(x['date'], x['product_symbol']), axis=1)

    for time in pnl_slippages:
        foba[f"passive_pnl_{time}s"] = np.where(foba.event_type==EventType.TRADE,
                    np.where(foba.side==Side.BID, 
                                (foba[f"midspot_{time}"]-foba.event_price)*(foba.event_volume*foba.contract_size),
                                    (foba.event_price-foba[f"midspot_{time}"])*(foba.event_volume*foba.contract_size))-foba.fees, 0)
        foba[f"aggressive_pnl_{time}s"] = np.where(foba.event_type==EventType.TRADE,
                    -1*np.where(foba.side==Side.BID, 
                                (foba[f"midspot_{time}"]-foba.event_price)*(foba.event_volume*foba.contract_size),
                                    (foba.event_price-foba[f"midspot_{time}"])*(trafobade_data.event_volume*foba.contract_size))-foba.fees, 0)
        foba[f"aggressive_bps_{time}s"] = 10000*(foba[f"aggressive_pnl_{time}s"]/(foba["event_volume"]*foba["event_price"]))
        foba[f"passive_bps_{time}s"] = 10000*(foba[f"passive_pnl_{time}s"]/(foba["event_volume"]*foba["event_price"]))

        foba[f"aggressive_tick_{time}s"] = (foba[f"aggressive_pnl_{time}s"]/(foba["event_volume"])/foba["tick_size"])
        foba[f"passive_tick_{time}s"] = (foba[f"passive_pnl_{time}s"]/(foba["event_volume"])/foba["tick_size"])

    foba['slipped_pnl_eod'] = np.where(foba['side'] == Side.BID, 
                                            (foba['eod_price'] - foba['event_price']) * foba['event_volume'] * foba['currency_rate'] - foba['fees'], 
                                            (foba['event_price'] - foba['eod_price']) * foba['event_volume'] * foba['currency_rate'] - foba['fees'])
    foba['aggressor_slipped_pnl_eod'] = np.where(foba['side'] == Side.BID, 
                                                    (foba['event_price'] - foba['eod_price']) * foba['event_volume'] * foba['currency_rate'] - foba['fees'], 
                                                    (foba['eod_price'] - foba['event_price']) * foba['event_volume'] * foba['currency_rate'] - foba['fees'])
    pnl_fields = [f"passive_pnl_{x}s" for x in pnl_slippages] + \
                [f"aggressive_pnl_{x}s" for x in pnl_slippages] + ["slipped_pnl_eod", "aggressor_slipped_pnl_eod"]
    currency_rate = 0.195
    foba[pnl_fields] = foba[pnl_fields] * currency_rate


def enrich_foba(foba, pnl_slippages=[5,15,30,60,120,240,300,600,900,1800,3600,7200]):
    foba = quoting_priority_enrichments(foba)
    foba = time_enrichments(foba)
    foba = enrich_level_metrics(foba)
    foba = enrich_liquidity(foba)
    foba = enrich_counterparty(foba)
    foba = credit_and_pnl_enrichment(foba)
    foba = enrich_buckets(foba)
    foba = pnl_enrichment(foba, pnl_slippages=pnl_slippages)
    return foba



