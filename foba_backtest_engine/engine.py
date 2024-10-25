import pandas as pd

from foba_backtest_engine.components.order_book.processors.foba_book_builder import (
    pybuilders,
)
from foba_backtest_engine.components.order_book.processors.foba_extract_foba_events import (
    extract_foba_events,
)
from foba_backtest_engine.components.order_book.utils.enums import Exchange
from foba_backtest_engine.components.order_book.utils.foba_competitor_broker_queue import (
    broker_orders_enrichment,
    competitor_enrichment,
    foreign_counterparty_enrichment,
    omdc_order_number_to_broker_number,
)
from foba_backtest_engine.components.order_book.utils.foba_credit_enricher import (
    event_enricher,
)
from foba_backtest_engine.components.order_book.utils.foba_fee_enrichment import (
    static_data_enrichment,
)
from foba_backtest_engine.components.order_book.utils.foba_feedstates import (
    feed_states_at_join,
    fetch_feed_stats_from_book_builders,
    full_feed_state_enrichment,
)
from foba_backtest_engine.components.order_book.utils.foba_hk_broker_data_fetch import (
    omdc_broker_number_to_name,
)
from foba_backtest_engine.components.order_book.utils.foba_misc_enrichments import (
    category_enrichment,
    derived_enrichment,
)
from foba_backtest_engine.components.order_book.utils.foba_omdc_broker_queue_processor import (
    omdc_broker_queue,
)
from foba_backtest_engine.components.order_book.utils.foba_own_orders import (
    get_optiver_trades,
    optiver_trade_and_quotes,
    order_delete_matches,
    order_deletes,
    order_matches,
    order_numbers_filtered,
    order_state_creates,
)
from foba_backtest_engine.components.order_book.utils.foba_slippages import (
    annotate_slippages,
)
from foba_backtest_engine.components.order_book.utils.foba_static_data_info import (
    static_data_info,
)
from foba_backtest_engine.components.order_book.utils.foba_time import (
    AverageSentTime,
    MaxSentTime,
    MinSentTime,
    TimeProfile,
    omdc_profile,
    send_times,
)
from foba_backtest_engine.enrichment import Enrichment, configure
from foba_backtest_engine.utils.base_utils import ImmutableRecord, get_logger
from foba_backtest_engine.utils.time_utils import start_end_time

logger = get_logger(__name__)


class Engine:
    def __init__(
        self,
        mode,
        book_ids,
        date,
        result_type="dataframe",
        config=None,
    ):
        if mode is None:
            raise ValueError(
                "Mode cannot be None. Please provide a valid mode: 'passive_analyis' | 'training' | 'simulation'"
            )
        if book_ids is None or not book_ids:
            raise ValueError(
                "Book IDs cannot be None or empty. Please provide valid book IDs."
            )
        if date is None:
            raise ValueError("Date cannot be None. Please provide a valid date.")
        self.mode = mode
        self.book_ids = book_ids
        self.date = date
        self.result_type = result_type
        self.config = config or {}
        self.results = None

    """
    ------------------
     (1) MODES
    ------------------

    This is the entry point into the different modes we can run. 
    i) We first generate a config ... this is a ImmutableDict 
    ii) We generate our processors ... these consume the config & do various actions on our input data
    iii) We then generate the enrichment from the processor(s)

    """

    def run(self, result_type="dataframe"):
        if self.mode == "passive_analysis":
            self._run_passive_analysis_mode(result_type)
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")

    def _run_passive_analysis_mode(self, result_type):
        logger.debug("Passive Analysis Mode: Started")
        configuration = self._generate_passive_analysis_configuration()
        processors = self._get_passive_analysis_processors(configuration)
        enriched_events = list(
            Enrichment(
                processors, configuration, auto_cleanup_resources=True
            ).joined_enrichments("foba_events")
        )

        logger.debug("Passive Analysis Mode: Completed")
        if result_type == "dataframe":
            FOBA_df = pd.DataFrame([record._asdict() for record in enriched_events])
            self.results = FOBA_df
        elif result_type == "list":
            self.results = enriched_events
        else:
            self.results = enriched_events
            logger.error(
                f"stored results as list - defaulted to this format as invalid result_type provided = {result_type}"
            )

    """
    ------------------
     (2) CONFIG GENERATION
    ------------------
    """

    def _generate_passive_analysis_configuration(self):
        max_workers = self.config.get("max_workers", 5)
        pybuilder_exchange = self.config.get("pybuilder_exchange", Exchange.OMDC)
        currency_rate = self.config.get("currency_rate", 1.0)
        book_ids = self.book_ids
        date_to_inspect = self.date
        days_ago = self.config.get("days_ago", 0)
        time_zone = self.config.get("time_zone", "Asia/Hong_Kong")
        end_hour = self.config.get("end_hour", 16)
        end_minute = self.config.get("end_minute", 1)
        exclude_pulls = self.config.get("exclude_pulls", False)
        include_only_optiver_pulls = self.config.get("include_only_optiver_pulls", True)
        exclude_inplace_updates = self.config.get("exclude_inplace_updates", True)
        book_build_parallel = self.config.get("book_build_parallel", False)
        conflated_broker_queue_path = self.config.get("conflated_broker_queue_path", "/Workspace/Users/kartikeya.bisht@optiver.com.au/FOBA_create/data/ConflatedBrokerQueue.parquet")
        order_book_path = self.config.get("order_book_path", "/Workspace/Users/kartikeya.bisht@optiver.com.au/FOBA_create/data/OrderBook.parquet")
        fee_info_path = self.config.get("fee_info_path", "/Workspace/Users/kartikeya.bisht@optiver.com.au/FOBA_create/FeeInfo.parquet")
        tick_schedule_path = self.config.get("tick_schedule_path", "/Workspace/Users/kartikeya.bisht@optiver.com.au/FOBA_create/TickSchedule.parquet")
        fee_schedule_path = self.config.get("fee_schedule_path", "/Workspace/Users/kartikeya.bisht@optiver.com.au/FOBA_create/FeeSchedule.parquet")
        order_insert_path=self.config.get("order_insert_path", "/Workspace/Users/kartikeya.bisht@optiver.com.au/FOBA_create/OrderInsert.parquet")
        delete_operation_path=self.config.get("delete_operation_path", "/Workspace/Users/kartikeya.bisht@optiver.com.au/FOBA_create/DeleteOperation.parquet")
        private_trade_path=self.config.get("private_trade_path", "/Workspace/Users/kartikeya.bisht@optiver.com.au/FOBA_create/PrivateTrade.parquet")
        private_feed_path=self.config.get("private_feed_path", "/Workspace/Users/kartikeya.bisht@optiver.com.au/FOBA_create/PrivateFeed.parquet")
        broker_mapping_path=self.config.get("broker_mapping_path", "/Workspace/Users/kartikeya.bisht@optiver.com.au/FOBA_create/BrokerMapping.parquet")
        
        sent_times = self.config.get(
            "sent_times",
            (
                MinSentTime(omdc_profile),
                MaxSentTime(omdc_profile),
                AverageSentTime(
                    TimeProfile(
                        min_one_way_delay=-float("inf"),
                        min_round_trip_delay=0.36e6,
                        max_one_way_delay=float("inf"),
                        max_round_trip_delay=0.50e6,
                    )
                ),
            ),
        )
        annotation_min_change = self.config.get("annotation_min_change", 0.001)
        exclude_lunch = self.config.get("exclude_lunch", True)
        pnl_slippage_times = self.config.get(
            "pnl_slippage_times",
            [5, 15, 30, 60, 120, 240, 300, 600, 900, 1800, 3600, 7200],
        )
        bbov_weights = self.config.get("bbov_weights", [1, 6, 6, 1])
        smooth_bbov_alpha = self.config.get("smooth_bbov_alpha", 0.10)
        bbov_interval_s = self.config.get("bbov_interval_s", 10)
        excluded_fee_names = self.config.get(
            "excluded_fee_names", ["Stock Full Stamp", "Stock Full Stamp - CBBC hedge"]
        )
        broker_mapping_backup = self.config.get("broker_mapping_backup", True)

        start_time, end_time = start_end_time(
            time_zone=time_zone,
            days_ago=days_ago,
            start_hour=6,
            start_minute=0,
            end_hour=end_hour,
            end_minute=end_minute,
            end_date=date_to_inspect,
        )

        filter_used = ImmutableRecord(
            start_time=start_time, end_time=end_time, book_ids=book_ids,
            order_insert_path=order_insert_path,
            delete_operation_path=delete_operation_path,
            private_trade_path=private_trade_path, 
            private_feed_path=private_feed_path,
            conflated_broker_queue_path=conflated_broker_queue_path,
            order_book_path = order_book_path,
            fee_info_path = fee_info_path,
            tick_schedule_path = tick_schedule_path,
            fee_schedule_path = fee_schedule_path,
            broker_mapping_path=broker_mapping_path
        )

        configuration = dict(
            filter=filter_used,
            max_workers=max_workers,
            pybuilder_exchange=pybuilder_exchange,
            book_build_parallel=book_build_parallel,
            tick_schedule_table_name="TickScheduleTickRules",
            sent_times=sent_times,
            annotation_min_change=annotation_min_change,
            exclude_lunch=exclude_lunch,
            pnl_slippage_times=pnl_slippage_times,
            bbov_weights=bbov_weights,
            smooth_bbov_alpha=smooth_bbov_alpha,
            bbov_interval_s=bbov_interval_s,
            aggressor_pnl_enrichment=True,
            currency_rate=currency_rate,
            early_cutoff_time=int(5e6),
            market_open=start_time.replace(hour=6, minute=0, second=0, microsecond=0),
            exclude_pulls=exclude_pulls,
            include_only_optiver_pulls=include_only_optiver_pulls,
            exclude_inplace_updates=exclude_inplace_updates,
            book_ids=book_ids,
            date_to_inspect=date_to_inspect,
            days_ago=days_ago,
            time_zone=time_zone,
            start_time=start_time,
            end_time=end_time,
            format="parquet",
            excluded_fee_names=excluded_fee_names,
            brokermap_backup=broker_mapping_backup,
        )
        return configuration

    """
    ------------------
     (3) PROCESSORS
    ------------------
    """
    def _get_passive_analysis_processors(self, configuration, enrich_competitor=True):
        processors = list()

        processors.append(order_numbers_filtered)
        processors.append(pybuilders)
        processors.append(
            configure(
                extract_foba_events,
                exclude_pulls=configuration["exclude_pulls"],
                include_only_optiver_pulls=configuration["include_only_optiver_pulls"],
                exclude_inplace_updates=configuration["exclude_inplace_updates"],
            )
        )
        processors.append(static_data_info)
        processors.append(static_data_enrichment)

        processors.append(send_times)

        processors.append(fetch_feed_stats_from_book_builders)
        processors.append(annotate_slippages)
        processors.append(full_feed_state_enrichment)
        processors.append(feed_states_at_join)

        processors.append(order_state_creates)
        processors.append(order_matches)
        processors.append(order_deletes)
        processors.append(order_delete_matches)
        
        processors.append(omdc_broker_number_to_name)

        processors.append(get_optiver_trades)
        processors.append(optiver_trade_and_quotes)

        processors.append(category_enrichment)
        processors.append(derived_enrichment)
        # processors.append(enrich_pnl)

        processors.append(event_enricher)

        if enrich_competitor:
            processors.append(omdc_broker_queue)
            processors.append(omdc_order_number_to_broker_number)
            processors.append(competitor_enrichment)
            processors.append(foreign_counterparty_enrichment)
            processors.append(broker_orders_enrichment)

        return processors
