from collections import namedtuple

StaticDataInfo = namedtuple(
    "StaticDataInfo", (
        'book_id',
            'product_symbol',
            'product_class',
            'contract_size',
            'exchange',
            'round_lot_size',
            'tick_schedule',
            'fee_rules'
    )
)

StaticDataEnrichment = namedtuple(
    'StaticDataEnrichment', (
        'exchange',
        'product_symbol',
        'product_class',
        'contract_size',
        'round_lot_size',
        'tick_size',
        'fees',
    )
)