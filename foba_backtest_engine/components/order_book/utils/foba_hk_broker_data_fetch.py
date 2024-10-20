from foba_backtest_engine.data.S3.S3OptiverResearchActions import OPTIVER_BUCKET_ACTIONS
from foba_backtest_engine.utils.base_utils import ImmutableDict
from foba_backtest_engine.enrichment import provides
from urllib.error import URLError
import pandas as pd
import urllib
import csv
import io

"""
HKEX publishes all SEHK broker Ids & their corresponding names as a downloadable csv
We download this and parse the csv - returning the broker number : name as a immutable dict
"""

def get_hk_broker_data_from_backup():
    path = "OMDC/Backup/BrokerMapping.parquet"
    # df = OPTIVER_BUCKET_ACTIONS.get_parquet(path=path)
    df = pd.read_parquet("/Users/kartikeyabisht/FobaBacktestEngine/temp_data/BrokerMapping.parquet")
    

    unique_participants = df['Participant Name'].unique()

    number_to_name = {}
    for name in unique_participants:
        broker_id = df[df['Participant Name'] == name]['Broker No.'][:1]
        broker_ids = broker_id.str.split(', ')
        for _, values in broker_ids.items():
            if values is None:
                pass
            else:
                for value in values:
                    if value:
                        number_to_name[int(value)] = name
    df = pd.DataFrame.from_dict(number_to_name, orient='index', columns=['broker_name'])
    df["broker_number"] = df.index
    df = df.sort_values(by='broker_number').reset_index(drop = True)
    return df


def get_hk_broker_data_from_hkex():
    url = 'https://www.hkex.com.hk/eng/PLW/csv/List_of_Current_SEHK_EP.CSV'
    webpage = urllib.request.urlopen(url)
    datareader = csv.reader(io.TextIOWrapper(webpage, encoding='utf-16'), delimiter='\t')

    data = []
    for row in datareader:
        data.append(row)
    df = pd.DataFrame(data)

    for col in range(len(df.columns)):
        df.rename(columns={col: df[col][0]}, inplace=True)
    df = df.drop(df.index[0])

    unique_participants = df['Participant Name'].unique()

    def items():
        for participant in unique_participants:
            broker_id = df[df['Participant Name'] == participant]['Broker No.'][:1]
            broker_ids = broker_id.str.split(', ')
            for _, values in broker_ids.items():
                for value in values:
                    if value:
                        yield int(value), participant
    broker_df = pd.DataFrame(list(items()), columns=['broker_number', 'broker_name'])
    broker_df.sort_values(by='broker_number', inplace=True)
    broker_df.reset_index(drop=True, inplace=True)
    return broker_df

def get_hk_broker_data(brokermap_backup=True):
    if brokermap_backup:
        return get_hk_broker_data_from_backup()
    else:
        try:
            return get_hk_broker_data_from_hkex()
        except Exception as e:
            print(f"Error fetching live broker mappings {e}")
            return get_hk_broker_data_from_backup()


@provides('broker_number_to_broker_name')
def omdc_broker_number_to_name():
    broker_data = get_hk_broker_data()
    if len(broker_data) > 0:
        return ImmutableDict(dict(zip(broker_data.broker_number, broker_data.broker_name)))
    else:
        return ImmutableDict()