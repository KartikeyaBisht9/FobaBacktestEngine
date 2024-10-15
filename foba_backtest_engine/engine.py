from foba_backtest_engine.components.order_book.utils.foba_own_orders import order_numbers_filtered, order_state_creates, manual_orders, order_matches, order_pick_offs, order_deletes, get_filled_hit_entries, hit_entry_enrichment_omdc, performance_classification
from foba_backtest_engine.components.order_book.utils.foba_competitor_broker_queue import omdc_order_number_to_broker_number, competitor_enrichment, foreign_counterparty_enrichment, broker_orders_enrichment
from foba_backtest_engine.components.order_book.utils.foba_feedstates import fetch_feed_stats_from_book_builders, full_feed_state_enrichment, feed_states_at_join
from foba_backtest_engine.components.order_book.utils.foba_time import TimeProfile, MinSentTime, MaxSentTime,AverageSentTime,omdc_profile, send_times
from foba_backtest_engine.components.order_book.utils.foba_misc_enrichments import category_enrichment, derived_enrichment
from foba_backtest_engine.components.order_book.utils.foba_hk_broker_data_fetch import omdc_broker_number_to_name
from foba_backtest_engine.components.order_book.utils.foba_omdc_broker_queue_processor import omdc_broker_queue
from foba_backtest_engine.components.order_book.processors.foba_extract_foba_events import extract_foba_events
from foba_backtest_engine.components.order_book.utils.foba_fee_enrichment import static_data_enrichment
from foba_backtest_engine.components.order_book.utils.foba_static_data_info import static_data_info
from foba_backtest_engine.components.order_book.utils.foba_credit_enricher import event_enricher
from foba_backtest_engine.components.order_book.utils.foba_slippages import annotate_slippages
from foba_backtest_engine.maintained_configs.HKEX_symbol_map import STOCK_SYMBOLS, SYMBOL_BOOK
from foba_backtest_engine.components.order_book.processors.foba_book_builder import pybuilders
from foba_backtest_engine.components.order_book.utils.foba_pnl_enrichment import enrich_pnl
from foba_backtest_engine.data.S3.S3OptiverResearchActions import OPTIVER_BUCKET_ACTIONS
from foba_backtest_engine.components.order_book.utils.foba_trades import optiver_trades
from foba_backtest_engine.utils.base_utils import get_logger, ImmutableRecord
from foba_backtest_engine.components.order_book.utils.enums import Exchange
from foba_backtest_engine.utils.time_utils import start_end_time
from foba_backtest_engine.enrichment import Enrichment, configure
import pandas as pd


logger = get_logger(__name__)

class Engine:
    def __init__(self, mode, book_ids, date, result_type="dataframe", s3_persist=True, s3_path="{exchange}/{dataType}/{date}.{format}", s3_persister=OPTIVER_BUCKET_ACTIONS, config=None):
        if mode is None:
            raise ValueError("Mode cannot be None. Please provide a valid mode: 'passive_analyis' | 'training' | 'simulation'")
        if book_ids is None or not book_ids:
            raise ValueError("Book IDs cannot be None or empty. Please provide valid book IDs.")
        if date is None:
            raise ValueError("Date cannot be None. Please provide a valid date.")
        self.mode = mode
        self.book_ids = book_ids
        self.date = date
        self.result_type = result_type
        self.s3_persist=s3_persist
        self.s3_path=s3_path
        self.s3_persister=s3_persister
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
        if self.mode == 'passive_analysis':
            self._run_passive_analysis_mode(result_type)
        elif self.mode == 'training':
            self._run_training_mode(result_type)
        elif self.mode == 'simulation':
            self._run_simulation_mode(result_type)
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")
    
    def _run_training_mode(self, result_type):
        # Implement training mode logic
        pass

    def _run_simulation_mode(self, result_type):
        # Implement simulation mode logic
        pass
    

    def _run_passive_analysis_mode(self, result_type):
        logger.debug('Passive Analysis Mode: Started')
        configuration = self._generate_passive_analysis_configuration()
        processors = self._get_passive_analysis_processors(configuration)
        enriched_events = list(Enrichment(
            processors,
            configuration,
            auto_cleanup_resources=True
        ).joined_enrichments('foba_events'))


        logger.debug('Passive Analysis Mode: Completed')
        if result_type == "dataframe":
            FOBA_df = pd.DataFrame([record._asdict() for record in enriched_events])
            self.results = FOBA_df
        elif result_type  == "list":
            self.results = enriched_events
        else:
            self.results = enriched_events
            logger.error(f"stored results as list - defaulted to this format as invalid result_type provided = {result_type}")
        
        if (configuration["s3_persist"]):
            if ((configuration["format"] == "parquet") & (result_type == "dataframe")):
                configuration["s3_persister"].put_parquet(path=configuration["s3_path"].format(exchange=configuration["pybuilder_exchange"].name, dataType='FOBA', date=self.date.date().isoformat(), format='parquet'), data=self.results)
            elif ((configuration["format"] == "parquet") & (result_type != "dataframe")):
                raise ValueError("cannot persist list of enrichment objects as parquet")
            elif ((configuration["format"] != "parquet") & (result_type == "dataframe")):
                raise ValueError("only parquet supported for now")
            else:
                raise ValueError(f"format = {configuration['format']} and result_type = {result_type}. Please set to parquet & dataframe")
        else:
            pass
            
    
    """
    ------------------
     (2) CONFIG GENERATION
    ------------------
    """
    def _generate_training_configuration(self):
        # Implement training mode logic
        pass

    def _generate_simulation_configuration(self):
        # Implement simulation mode logic
        pass


    def _generate_passive_analysis_configuration(self):
        max_workers = self.config.get('max_workers', 5)
        pybuilder_exchange = self.config.get('pybuilder_exchange', Exchange.OMDC)
        currency_rate = self.config.get('currency_rate', 1.0)
        book_ids = self.book_ids
        date_to_inspect = self.date
        days_ago = self.config.get('days_ago', 0)
        time_zone = self.config.get('time_zone', 'Asia/Hong_Kong')
        end_hour = self.config.get('end_hour', 16)
        end_minute = self.config.get('end_minute', 1)
        exclude_pulls = self.config.get('exclude_pulls', False)
        include_only_optiver_pulls = self.config.get('include_only_optiver_pulls', True)
        exclude_inplace_updates = self.config.get('exclude_inplace_updates', True)
        book_build_parallel = self.config.get('book_build_parallel', False)
        sent_times=self.config.get("sent_times", (
            MinSentTime(omdc_profile),
            MaxSentTime(omdc_profile),
            AverageSentTime(
                TimeProfile(
                    min_one_way_delay=-float('inf'),
                    min_round_trip_delay=float(0.36e6),
                    max_one_way_delay=float('inf'),
                    max_round_trip_delay=float(0.50e6))
                        )
                    )
                )
        annotation_min_change=self.config.get("annotation_min_change",0.001)
        exclude_lunch=self.config.get("exclude_lunch", True)
        pnl_slippage_times=self.config.get("pnl_slippage_times", [5,15,30,60,120,240,300,600,900,1800,3600,7200])
        bbov_weights=self.config.get("bbov_weights",[1,6,6,1])
        smooth_bbov_alpha=self.config.get("smooth_bbov_alpha",0.10)
        bbov_interval_s=self.config.get("bbov_interval_s", 10)


        start_time, end_time = start_end_time(time_zone=time_zone,
                                          days_ago=days_ago,
                                          start_hour=6,
                                          start_minute=0,
                                          end_hour=end_hour,
                                          end_minute=end_minute,
                                          end_date=date_to_inspect)
        
        filter_used = ImmutableRecord(start_time=start_time, end_time=end_time, book_ids=book_ids)
        
        configuration = dict(
            filter=filter_used,
            max_workers = max_workers,
            pybuilder_exchange = pybuilder_exchange,
            book_build_parallel=book_build_parallel,
            tick_schedule_table_name='TickScheduleTickRules',
            sent_times=sent_times,
            annotation_min_change=annotation_min_change,
            exclude_lunch = exclude_lunch, 
            pnl_slippage_times=pnl_slippage_times,
            bbov_weights=bbov_weights, 
            smooth_bbov_alpha =smooth_bbov_alpha,
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
            s3_persist=self.s3_persist,
            s3_path=self.s3_path,
            s3_persister=self.s3_persister,
            format="parquet"
        )

        return configuration
    
    """
    ------------------
     (3) PROCESSORS
    ------------------
    """

    def _get_training_processors(self, configuration):

        return None
    

    def _get_simulation_processors(self, configuration):
        processors = list()
        """
        1) Gets the FobaEvents & FeedStates
        """
        processors.append(pybuilders)           
        processors.append(configure(extract_foba_events,
                                    exclude_pulls=configuration['exclude_pulls'],
                                    include_only_optiver_pulls=configuration['include_only_optiver_pulls'],
                                    exclude_inplace_updates=configuration['exclude_inplace_updates']))
        processors.append(static_data_info)
        processors.append(static_data_enrichment)
        processors.append(fetch_feed_stats_from_book_builders)
        processors.append(full_feed_state_enrichment)
        processors.append(category_enrichment)
        processors.append(derived_enrichment)
        """
        2) Applies OMDC broker queue tagging (passive tagging only)
        """
        processors.append(send_times)
        processors.append(omdc_broker_number_to_name)
        processors.append(omdc_broker_queue)
        processors.append(omdc_order_number_to_broker_number)
        processors.append(competitor_enrichment)
        processors.append(foreign_counterparty_enrichment)
        processors.append(broker_orders_enrichment)
        """
        3) Valuation processors
        """
        # 1) need a processor that creates the "smart_skew" | "smart_party_passive_position" | "support_abn_priority" | "resistance_abn_priority"
        # 2) need a processor that calculates various different features
        # 3) need a relative value creator = this uses midspot dataframe for HSI/HHI & A-shares & correct timestamping logic to create these features
        # 4) need a target annotator

        return processors
    
    
    def _get_passive_analysis_processors(
            self,
            configuration,
            enrich_competitor=True
            ):
        """
        1. order_numbers_filtered   -- gets all of our own orderIds 
        2. pybuilders               -- uses ADD | MODIFY | DELETE & CLEAR orders to generate & build the orderBook
        3. extract_foba_events      -- extracts out FobaEvent objects ... these will make the row of our analysed df & are enriched by other processors
        4. static_data_info         -- we initialize static information like tickSize, fees etc
        5. static_data_enrichment   -- we enrich the FobaEvents with fees (HKD)
        6. order_state_creates      -- gets all the orders we created
        7. manual_orders            -- gets all of our manual orders
        
        Important for SELF analysis ...
        8.  order_matches           -- we create OptiverOrderMatch objects
        9.  order_deletes           -- we create OptiverOrderDelete objects
        10. order_pick_offs         -- we create OptiverPickOffs objects
        11. get_filled_hit_entires  -- all hit attempts that are filled
        12. hit_entry_enrichment    -- we create OptiverHits objects
        13. performance_classification  -- we create OptiverPerformance objects

        Now associated to EACH fobe_event (FobaEvent) we have various objects (namedtuple type) that give us self analysis metrics like performance_classification etc

        14. send_times              -- use w2w and one way delays to calculate max and average send time for orders
        15. 

        """
        processors = list()

        processors.append(order_numbers_filtered)
        processors.append(pybuilders)           
        processors.append(configure(extract_foba_events,
                                    exclude_pulls=configuration['exclude_pulls'],
                                    include_only_optiver_pulls=configuration['include_only_optiver_pulls'],
                                    exclude_inplace_updates=configuration['exclude_inplace_updates']))
        processors.append(static_data_info)
        processors.append(static_data_enrichment)

        # TODO: The processors below require calibration w/ optiver data
        """
        order_numbers_filtered  ||  manual_orders   ||  order_matches   ||  order_pulls   ||  order_pick_offs  ||  order_pick_offs  ||  filled_hits   ||  hit_entry  ||  missed_quote_reason

        What are they doing?
            - order_state_creates & manual_orders just fetch all orders that optiver was involved in
            - order_matches, deletes, pick_offs, filled_hit_entry & hit_enrichment create respective objects for EACH event_id in foba_events
            - these events have information about whether the event was an optiver order, if it was a delete, if we were picked off etc
        """
        processors.append(order_state_creates)
        processors.append(manual_orders)

        processors.append(order_matches)
        processors.append(order_deletes)
        processors.append(order_pick_offs)
        processors.append(get_filled_hit_entries)
        processors.append(hit_entry_enrichment_omdc)
        processors.append(performance_classification)
        
        processors.append(send_times)

        processors.append(fetch_feed_stats_from_book_builders)
        processors.append(annotate_slippages)
        processors.append(full_feed_state_enrichment)
        processors.append(feed_states_at_join)

        processors.append(category_enrichment)
        processors.append(derived_enrichment)
        processors.append(enrich_pnl)

        processors.append(event_enricher)

        if enrich_competitor:
            processors.append(optiver_trades)
            processors.append(omdc_broker_number_to_name)
            processors.append(omdc_broker_queue)
            processors.append(omdc_order_number_to_broker_number)
            processors.append(competitor_enrichment)
            processors.append(foreign_counterparty_enrichment)
            processors.append(broker_orders_enrichment)

        return processors