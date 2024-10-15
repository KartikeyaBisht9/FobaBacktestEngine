from foba_backtest_engine.utils.base_utils import ImmutableRecord

"""
The symbols below use Hitting Jobbing
"""
HIT_JOBBING = {
    'AAC': ImmutableRecord(book_id=2018, index='HSI', bloomberg_code='2018 HK Equity', group='STOCK', sector='tech'),
    'ABC': ImmutableRecord(book_id=1288, index='HHI', bloomberg_code='1288 HK Equity', group='STOCK', sector='banks'),
    'ACC': ImmutableRecord(book_id=914, index='HHI', bloomberg_code='914 HK Equity', group='STOCK', sector='construction'),
    'AIA': ImmutableRecord(book_id=1299, index='HSI', bloomberg_code='1299 HK Equity', group='STOCK', sector='insurers'),
    'ALB': ImmutableRecord(book_id=9988, index='HHI', bloomberg_code='9988 HK Equity', group='STOCK', sector='tech'),
    'ALC': ImmutableRecord(book_id=2600, index='HHI', bloomberg_code='2600 HK Equity', group='STOCK', sector='mining'),
    'ALH': ImmutableRecord(book_id=241, index='HHI', bloomberg_code='241 HK Equity', group='STOCK', sector='tech'),
    'ANA': ImmutableRecord(book_id=2020, index='HHI', bloomberg_code='2020 HK Equity', group='STOCK', sector='consumer'),
    'BCL': ImmutableRecord(book_id=3988, index='HHI', bloomberg_code='3988 HK Equity', group='STOCK', sector='banks'),
    'BCM': ImmutableRecord(book_id=3328, index='HHI', bloomberg_code='3328 HK Equity', group='STOCK', sector='banks'),
    'BIU': ImmutableRecord(book_id=9888, index='HHI', bloomberg_code='9888 HK Equity', group='STOCK', sector='tech'),
    'BLI': ImmutableRecord(book_id=9626, index='HHI', bloomberg_code='9626 HK Equity', group='STOCK', sector='tech'),
    'BOC': ImmutableRecord(book_id=2388, index='HSI', bloomberg_code='2388 HK Equity', group='STOCK', sector='banks'),
    'BUD': ImmutableRecord(book_id=1876, index='HSI', bloomberg_code='1876 HK Equity', group='STOCK', sector='consumer'),
    'BYD': ImmutableRecord(book_id=1211, index='HHI', bloomberg_code='1211 HK Equity', group='STOCK', sector='auto'),
    'BYE': ImmutableRecord(book_id=285, index='HHI', bloomberg_code='285 HK Equity', group='STOCK', sector='tech'),
    'CCB': ImmutableRecord(book_id=939, index='HHI', bloomberg_code='939 HK Equity', group='STOCK', sector='banks'),
    'CCC': ImmutableRecord(book_id=1800, index='HHI', bloomberg_code='1800 HK Equity', group='STOCK', sector='construction'),
    'CHT': ImmutableRecord(book_id=941, index='HSI', bloomberg_code='941 HK Equity', group='STOCK', sector='telco'),
    'CHU': ImmutableRecord(book_id=762, index='HSI', bloomberg_code='762 HK Equity', group='STOCK', sector='telco'),
    'CKH': ImmutableRecord(book_id=1, index='HSI', bloomberg_code='1 HK Equity', group='STOCK', sector='properties'),
    'CKP': ImmutableRecord(book_id=1113, index='HSI', bloomberg_code='1113 HK Equity', group='STOCK', sector='properties'),
    'CLI': ImmutableRecord(book_id=2628, index='HHI', bloomberg_code='2628 HK Equity', group='STOCK', sector='insurers'),
    'CLP': ImmutableRecord(book_id=2, index='HSI', bloomberg_code='2 HK Equity', group='STOCK', sector='utilities'),
    'CMB': ImmutableRecord(book_id=3968, index='HHI', bloomberg_code='3968 HK Equity', group='STOCK', sector='banks'),
    'CNC': ImmutableRecord(book_id=883, index='HSI', bloomberg_code='883 HK Equity', group='STOCK', sector='oils'),
    'COG': ImmutableRecord(book_id=2007, index='HSI', bloomberg_code='2007 HK Equity', group='STOCK', sector='properties'),
    'COL': ImmutableRecord(book_id=688, index='HSI', bloomberg_code='688 HK Equity', group='STOCK', sector='properties'),
    'COS': ImmutableRecord(book_id=1919, index='HHI', bloomberg_code='1919 HK Equity', group='STOCK', sector='consumer'),
    'CPC': ImmutableRecord(book_id=386, index='HHI', bloomberg_code='386 HK Equity', group='STOCK', sector='oils'),
    'CPI': ImmutableRecord(book_id=2601, index='HHI', bloomberg_code='2601 HK Equity', group='STOCK', sector='insurers'),
    'CRL': ImmutableRecord(book_id=1109, index='HSI', bloomberg_code='1109 HK Equity', group='STOCK', sector='properties'),
    'CSE': ImmutableRecord(book_id=1088, index='HHI', bloomberg_code='1088 HK Equity', group='STOCK', sector='mining'),
    'CSP': ImmutableRecord(book_id=1093, index='HSI', bloomberg_code='1093 HK Equity', group='STOCK', sector='pharma'),
    'CTB': ImmutableRecord(book_id=998, index='HHI', bloomberg_code='998 HK Equity', group='STOCK', sector='banks'),
    'CTC': ImmutableRecord(book_id=728, index='HHI', bloomberg_code='728 HK Equity', group='STOCK', sector='telco'),
    'CTS': ImmutableRecord(book_id=6030, index='HHI', bloomberg_code='6030 HK Equity', group='STOCK', sector='markets'),
    'EVG': ImmutableRecord(book_id=3333, index='HSI', bloomberg_code='3333 HK Equity', group='STOCK', sector='properties'),
    'GAH': ImmutableRecord(book_id=175, index='HSI', bloomberg_code='175 HK Equity', group='STOCK', sector='auto'),
    'GHL': ImmutableRecord(book_id=868, index='HSI', bloomberg_code='868 HK Equity', group='STOCK', sector='construction'),
    'GLI': ImmutableRecord(book_id=1772, index='HHI', bloomberg_code='1772 HK Equity', group='STOCK', sector='mining'),
    'GLX': ImmutableRecord(book_id=27, index='HSI', bloomberg_code='27 HK Equity', group='STOCK', sector='casinos'),
    'GWM': ImmutableRecord(book_id=2333, index='HHI', bloomberg_code='2333 HK Equity', group='STOCK', sector='auto'),
    'HAI': ImmutableRecord(book_id=6837, index='HHI', bloomberg_code='6837 HK Equity', group='STOCK', sector='markets'),
    'HDO': ImmutableRecord(book_id=6862, index='HSI', bloomberg_code='6862 HK Equity', group='STOCK', sector='consumer'),
    'HEH': ImmutableRecord(book_id=6, index='HSI', bloomberg_code='6 HK Equity', group='STOCK', sector='utilities'),
    'HEX': ImmutableRecord(book_id=388, index='HSI', bloomberg_code='388 HK Equity', group='STOCK', sector='markets'),
    'HGN': ImmutableRecord(book_id=1044, index='HHI', bloomberg_code='1044 HK Equity', group='STOCK', sector='consumer'),
    'HKB': ImmutableRecord(book_id=5, index='HSI', bloomberg_code='5 HK Equity', group='STOCK', sector='banks'),
    'HKG': ImmutableRecord(book_id=3, index='HSI', bloomberg_code='3 HK Equity', group='STOCK', sector='utilities'),
    'HLD': ImmutableRecord(book_id=12, index='HSI', bloomberg_code='12 HK Equity', group='STOCK', sector='properties'),
    'HSB': ImmutableRecord(book_id=11, index='HSI', bloomberg_code='11 HK Equity', group='STOCK', sector='banks'),
    'ICB': ImmutableRecord(book_id=1398, index='HHI', bloomberg_code='1398 HK Equity', group='STOCK', sector='banks'),
    'INB': ImmutableRecord(book_id=1801, index='HHI', bloomberg_code='1801 HK Equity', group='STOCK', sector='unknown'),
    'JDC': ImmutableRecord(book_id=9618, index='HHI', bloomberg_code='9618 HK Equity', group='STOCK', sector='tech'),
    'JDH': ImmutableRecord(book_id=6618, index='HHI', bloomberg_code='6618 HK Equity', group='STOCK', sector='tech'),
    'JXC': ImmutableRecord(book_id=358, index='HHI', bloomberg_code='358 HK Equity', group='STOCK', sector='mining'),
    'KDS': ImmutableRecord(book_id=268, index='HHI', bloomberg_code='268 HK Equity', group='STOCK', sector='tech'),
    'KSO': ImmutableRecord(book_id=3888, index='HSI', bloomberg_code='3888 HK Equity', group='STOCK', sector='tech'),
    'KST': ImmutableRecord(book_id=1024, index='HHI', bloomberg_code='1024 HK Equity', group='STOCK', sector='tech'),
    'LAU': ImmutableRecord(book_id=2015, index='HHI', bloomberg_code='2015 HK Equity', group='STOCK', sector='auto'),    
    'LEN': ImmutableRecord(book_id=992, index='HSI', bloomberg_code='992 HK Equity', group='STOCK', sector='tech'),
    'LNI': ImmutableRecord(book_id=2331, index='HHI', bloomberg_code='2331 HK Equity', group='STOCK', sector='consumer'),
    'LNK': ImmutableRecord(book_id=823, index='HSI', bloomberg_code='823 HK Equity', group='STOCK', sector='properties'),
    'MEN': ImmutableRecord(book_id=2319, index='HSI', bloomberg_code='2319 HK Equity', group='STOCK', sector='consumer'),
    'MET': ImmutableRecord(book_id=3690, index='HSI', bloomberg_code='3690 HK Equity', group='STOCK', sector='tech'),
    'MIU': ImmutableRecord(book_id=1810, index='HSI', bloomberg_code='1810 HK Equity', group='STOCK', sector='tech'),
    'MOL': ImmutableRecord(book_id=3993, index='HHI', bloomberg_code='3993 HK Equity', group='STOCK', sector='mining'),
    'MSB': ImmutableRecord(book_id=1988, index='HHI', bloomberg_code='1988 HK Equity', group='STOCK', sector='banks'),
    'MTR': ImmutableRecord(book_id=66, index='HSI', bloomberg_code='66 HK Equity', group='STOCK', sector='properties'),
    'NBM': ImmutableRecord(book_id=3323, index='HHI', bloomberg_code='3323 HK Equity', group='STOCK', sector='construction'),
    'NCL': ImmutableRecord(book_id=1336, index='HHI', bloomberg_code='1336 HK Equity', group='STOCK', sector='insurers'),
    'NFU': ImmutableRecord(book_id=9633, index='HHI', bloomberg_code='9633 HK Equity', group='STOCK', sector='consumer'),
    'NTE': ImmutableRecord(book_id=9999, index='HHI', bloomberg_code='9999 HK Equity', group='STOCK', sector='tech'),
    'NWD': ImmutableRecord(book_id=17, index='HSI', bloomberg_code='17 HK Equity', group='STOCK', sector='properties'),
    'PAI': ImmutableRecord(book_id=2318, index='HHI', bloomberg_code='2318 HK Equity', group='STOCK', sector='insurers'),
    'PEC': ImmutableRecord(book_id=857, index='HHI', bloomberg_code='857 HK Equity', group='STOCK', sector='oils'),
    'PEN': ImmutableRecord(book_id=9868, index='HHI', bloomberg_code='9868 HK Equity', group='STOCK', sector='auto'),
    'PHT': ImmutableRecord(book_id=1833, index='HHI', bloomberg_code='1833 HK Equity', group='STOCK', sector='tech'),
    'PIC': ImmutableRecord(book_id=2328, index='HHI', bloomberg_code='2328 HK Equity', group='STOCK', sector='insurers'),
    'SAN': ImmutableRecord(book_id=1928, index='HSI', bloomberg_code='1928 HK Equity', group='STOCK', sector='casinos'),
    'SBO': ImmutableRecord(book_id=1177, index='HHI', bloomberg_code='1177 HK Equity', group='STOCK', sector='consumer'),
    'SET': ImmutableRecord(book_id=20, index='HHI', bloomberg_code='20 HK Equity', group='STOCK', sector='unknown'),
    'SHK': ImmutableRecord(book_id=16, index='HSI', bloomberg_code='16 HK Equity', group='STOCK', sector='properties'),
    'SHL': ImmutableRecord(book_id=968, index='HSI', bloomberg_code='968 HK Equity', group='STOCK', sector='utilities'),
    'SHZ': ImmutableRecord(book_id=2313, index='HHI', bloomberg_code='2313 HK Equity', group='STOCK', sector='consumer'),
    'SMC': ImmutableRecord(book_id=981, index='HHI', bloomberg_code='981 HK Equity', group='STOCK', sector='tech'),
    'SNO': ImmutableRecord(book_id=2382, index='HSI', bloomberg_code='2382 HK Equity', group='STOCK', sector='tech'),
    'SUN': ImmutableRecord(book_id=1918, index='HSI', bloomberg_code='1918 HK Equity', group='STOCK', sector='properties'),
    'TCH': ImmutableRecord(book_id=700, index='HSI', bloomberg_code='700 HK Equity', group='STOCK', sector='tech'),
    'TIC': ImmutableRecord(book_id=669, index='HSI', bloomberg_code='669 HK Equity', group='STOCK', sector='consumer'),
    'TRP': ImmutableRecord(book_id=9961, index='HHI', bloomberg_code='9961 HK Equity', group='STOCK', sector='tech'),
    'TWR': ImmutableRecord(book_id=788, index='HSI', bloomberg_code='788 HK Equity', group='STOCK', sector='telco'),
    'VNK': ImmutableRecord(book_id=2202, index='HHI', bloomberg_code='2202 HK Equity', group='STOCK', sector='properties'),
    'WHL': ImmutableRecord(book_id=4, index='HSI', bloomberg_code='4 HK Equity', group='STOCK', sector='properties'),
    'WXB': ImmutableRecord(book_id=2269, index='HSI', bloomberg_code='2269 HK Equity', group='STOCK', sector='pharma'),
    'XPB': ImmutableRecord(book_id=1658, index='HSI', bloomberg_code='1658 HK Equity', group='STOCK', sector='banks'),
    'YZC': ImmutableRecord(book_id=1171, index='HHI', bloomberg_code='1171 HK Equity', group='STOCK', sector='mining'),
    'ZAO': ImmutableRecord(book_id=6060, index='HHI', bloomberg_code='6060 HK Equity', group='STOCK', sector='insurers'),
    'ZSH': ImmutableRecord(book_id=881, index='HHI', bloomberg_code='881 HK Equity', group='STOCK', sector='unknown'),
    'HSI': ImmutableRecord(book_id=None, index='HSI', bloomberg_code='HSI Index', group='FUTURES_HEDGE', sector='unknown'),
    'HHI': ImmutableRecord(book_id=None, index='HHI', bloomberg_code='HSCEI Index', group='FUTURES_HEDGE', sector='unknown'),
    '2822': ImmutableRecord(book_id=2822, index='A50', bloomberg_code='2822 HK Equity', group='ETF', sector='unknown'),
    '2823': ImmutableRecord(book_id=2823, index='A50', bloomberg_code='2823 HK Equity', group='ETF', sector='unknown'),
    '101': ImmutableRecord(book_id=101, index='HSI', bloomberg_code='101 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '1038': ImmutableRecord(book_id=1038, index='HSI', bloomberg_code='1038 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '1099': ImmutableRecord(book_id=1099, index='HHI', bloomberg_code='1099 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'CRC': ImmutableRecord(book_id=1186, index='HHI', bloomberg_code='1186 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'PIN': ImmutableRecord(book_id=1339, index='', bloomberg_code='1339 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'KLE': ImmutableRecord(book_id=135, index='', bloomberg_code='135 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'CDA': ImmutableRecord(book_id=1359, index='', bloomberg_code='1359 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '144': ImmutableRecord(book_id=144, index='', bloomberg_code='144 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'WWC': ImmutableRecord(book_id=151, index='HSI', bloomberg_code='151 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '1766': ImmutableRecord(book_id=1766, index='', bloomberg_code='1766 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'SWA': ImmutableRecord(book_id=19, index='HSI', bloomberg_code='19 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '2202': ImmutableRecord(book_id=2202, index='HHI', bloomberg_code='2202 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'BEA': ImmutableRecord(book_id=23, index='', bloomberg_code='23 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'CIT': ImmutableRecord(book_id=267, index='HSI', bloomberg_code='267 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'CRG': ImmutableRecord(book_id=390, index='HHI', bloomberg_code='390 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'DFM': ImmutableRecord(book_id=489, index='', bloomberg_code='489 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '6886': ImmutableRecord(book_id=6886, index='', bloomberg_code='6886 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '83': ImmutableRecord(book_id=83, index='HSI', bloomberg_code='83 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '669': ImmutableRecord(book_id=669, index='HSI', bloomberg_code='669 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '288': ImmutableRecord(book_id=288, index='HSI', bloomberg_code='288 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '2883': ImmutableRecord(book_id=2883, index='', bloomberg_code='2883 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '6818': ImmutableRecord(book_id=6818, index='', bloomberg_code='6818 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '683': ImmutableRecord(book_id=683, index='', bloomberg_code='683 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '813': ImmutableRecord(book_id=813, index='', bloomberg_code='813 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '960': ImmutableRecord(book_id=960, index='', bloomberg_code='960 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '3377': ImmutableRecord(book_id=3377, index='', bloomberg_code='3377 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '3799': ImmutableRecord(book_id=3799, index='', bloomberg_code='3799 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '6808': ImmutableRecord(book_id=6808, index='', bloomberg_code='6808 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '966': ImmutableRecord(book_id=966, index='HHI', bloomberg_code='966 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '384': ImmutableRecord(book_id=384, index='', bloomberg_code='384 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '656': ImmutableRecord(book_id=656, index='HHI', bloomberg_code='656 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '270': ImmutableRecord(book_id=270, index='', bloomberg_code='270 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '2799': ImmutableRecord(book_id=2799, index='', bloomberg_code='2799 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '2386': ImmutableRecord(book_id=2386, index='', bloomberg_code='2386 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '3899': ImmutableRecord(book_id=3899, index='', bloomberg_code='3899 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '934': ImmutableRecord(book_id=934, index='', bloomberg_code='934 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '2238': ImmutableRecord(book_id=2238, index='', bloomberg_code='2238 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '200': ImmutableRecord(book_id=200, index='', bloomberg_code='200 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '582': ImmutableRecord(book_id=582, index='', bloomberg_code='582 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '880': ImmutableRecord(book_id=880, index='', bloomberg_code='880 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '1128': ImmutableRecord(book_id=1128, index='', bloomberg_code='1128 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'MGM': ImmutableRecord(book_id=2282, index='', bloomberg_code='2282 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '3918': ImmutableRecord(book_id=3918, index='', bloomberg_code='3918 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '303': ImmutableRecord(book_id=303, index='', bloomberg_code='303 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '552': ImmutableRecord(book_id=552, index='', bloomberg_code='552 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '698': ImmutableRecord(book_id=698, index='', bloomberg_code='698 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '732': ImmutableRecord(book_id=732, index='', bloomberg_code='732 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '763': ImmutableRecord(book_id=763, index='', bloomberg_code='763 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    'FIH': ImmutableRecord(book_id=2038, index='', bloomberg_code='2038 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '460': ImmutableRecord(book_id=460, index='', bloomberg_code='460 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '570': ImmutableRecord(book_id=570, index='', bloomberg_code='570 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '867': ImmutableRecord(book_id=867, index='', bloomberg_code='867 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '1193': ImmutableRecord(book_id=1193, index='HHI', bloomberg_code='1193 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '1530': ImmutableRecord(book_id=1530, index='', bloomberg_code='1530 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '2186': ImmutableRecord(book_id=2186, index='', bloomberg_code='2186 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '2607': ImmutableRecord(book_id=2607, index='', bloomberg_code='2607 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '3320': ImmutableRecord(book_id=3320, index='', bloomberg_code='3320 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '400': ImmutableRecord(book_id=400, index='', bloomberg_code='400 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '799': ImmutableRecord(book_id=799, index='', bloomberg_code='799 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '1357': ImmutableRecord(book_id=1357, index='', bloomberg_code='1357 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '1980': ImmutableRecord(book_id=1980, index='', bloomberg_code='1980 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '1997': ImmutableRecord(book_id=1997, index='HSI', bloomberg_code='1997 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '836': ImmutableRecord(book_id=836, index='', bloomberg_code='836 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '291': ImmutableRecord(book_id=291, index='HHI', bloomberg_code='291 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '2688': ImmutableRecord(book_id=2688, index='HHI', bloomberg_code='2688 HK Equity', group='STOCK_NOT_TRADED', sector='unknown'),
    '586': ImmutableRecord(book_id=586, index='HHI', bloomberg_code='586 HK Equity', group='STOCK_NOT_TRADED', sector='unknown') }

"""
The symbols below we use Quoting Jobbing for
"""

QUOTE_JOBBING_ONLY = {
    'EV35': ImmutableRecord(book_id=35, group='STOCK', index='HSI', bloomberg_code='35 HK Equity', sector='unknown'),
    'EV101': ImmutableRecord(book_id=101, group='STOCK', index='HSI', bloomberg_code='101 HK Equity', sector='unknown'),
    'EV267': ImmutableRecord(book_id=267, group='STOCK', index='HSI', bloomberg_code='267 HK Equity', sector='unknown'),
    'EV288': ImmutableRecord(book_id=288, group='STOCK', index='HSI', bloomberg_code='288 HK Equity', sector='unknown'),
    'EV291': ImmutableRecord(book_id=291, group='STOCK', index='HSI', bloomberg_code='291 HK Equity', sector='unknown'),
    'EV316': ImmutableRecord(book_id=316, group='STOCK', index='HSI', bloomberg_code='316 HK Equity', sector='unknown'),
    'EV836': ImmutableRecord(book_id=836, group='STOCK', index='HSI', bloomberg_code='836 HK Equity', sector='unknown'),
    'EV960': ImmutableRecord(book_id=960, group='STOCK', index='HSI', bloomberg_code='960 HK Equity', sector='unknown'),
    'EV1038': ImmutableRecord(book_id=1038, group='STOCK', index='HSI', bloomberg_code='1038 HK Equity', sector='unknown'),
    'EV1099': ImmutableRecord(book_id=1099, group='STOCK', index='HSI', bloomberg_code='1099 HK Equity', sector='unknown'),
    'EV1209': ImmutableRecord(book_id=1209, group='STOCK', index='HSI', bloomberg_code='1209 HK Equity', sector='unknown'),
    'EV1378': ImmutableRecord(book_id=1378, group='STOCK', index='HSI', bloomberg_code='1378 HK Equity', sector='unknown'),
    'EV1929': ImmutableRecord(book_id=1929, group='STOCK', index='HSTI', bloomberg_code='1929 HK Equity', sector='unknown'),
    'EV2359': ImmutableRecord(book_id=2359, group='STOCK', index='HSI', bloomberg_code='2359 HK Equity', sector='unknown'),
    'EV2688': ImmutableRecord(book_id=2688, group='STOCK', index='HSI', bloomberg_code='2688 HK Equity', sector='unknown'),
    'EV2899': ImmutableRecord(book_id=2899, group='STOCK', index='HSI', bloomberg_code='2899 HK Equity', sector='unknown'),
    'EV3692': ImmutableRecord(book_id=3692, group='STOCK', index='HSI', bloomberg_code='3692 HK Equity', sector='unknown'),
    'EV6098': ImmutableRecord(book_id=6098, group='STOCK', index='HSI', bloomberg_code='6098 HK Equity', sector='unknown'),
    'EV6690': ImmutableRecord(book_id=6690, group='STOCK', index='HSI', bloomberg_code='6690 HK Equity', sector='unknown'),
    'EV1347': ImmutableRecord(book_id=1347, group='STOCK', index='HSI', bloomberg_code='1347 HK Equity', sector='unknown'),
    'EV1797': ImmutableRecord(book_id=1797, group='STOCK', index='HSTI', bloomberg_code='1797 HK Equity', sector='unknown'),
    'EV322': ImmutableRecord(book_id=322, group='STOCK', index='HSTI', bloomberg_code='322 HK Equity', sector='unknown'),
    'EV772': ImmutableRecord(book_id=772, group='STOCK', index='HSTI', bloomberg_code='772 HK Equity', sector='unknown'),
    'EV9698': ImmutableRecord(book_id=9698, group='STOCK', index='HSTI', bloomberg_code='9698 HK Equity', sector='unknown'),
    'EV9866': ImmutableRecord(book_id=9866, group='STOCK', index='HSTI', bloomberg_code='9866 HK Equity', sector='unknown'),
    'EV9898': ImmutableRecord(book_id=9898, group='STOCK', index='HSTI', bloomberg_code='9898 HK Equity', sector='unknown')
}

HK_STOCK_DICT = {**HIT_JOBBING, **QUOTE_JOBBING_ONLY}

STOCK_SYMBOLS = sorted(
    [
        k
        for k, v in HK_STOCK_DICT.items()
        if v.group == "STOCK" 
    ]
)

HSI_SYMBOLS = [
    k for k, v in HK_STOCK_DICT.items() if v.index == "HSI" and v.group == "STOCK"
]

HHI_SYMBOLS = [
    k for k, v in HK_STOCK_DICT.items() if v.index == "HHI" and v.group == "STOCK"
]

NON_TRADED_STOCK_SYMBOLS = sorted(
    [k for k, v in HK_STOCK_DICT.items() if v.group == "STOCK_NOT_TRADED"]
)

# Simplifying the symbol mappings as per your requirement
SYMBOL_BOOK = {
    k: v.book_id
    for k, v in HK_STOCK_DICT.items()
}

SYMBOL_STOCK_CODE = {
    k: v.book_id for k, v in HK_STOCK_DICT.items()
}

STOCK_CODE_TO_SYMBOL = {
    v.book_id: k for k, v in HK_STOCK_DICT.items()
}

BLOOMBERG_INDEX_MAP = {v.bloomberg_code: k for k, v in HK_STOCK_DICT.items()}
