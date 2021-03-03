import os
import sys
import time
import argparse
import glob
import re
from logging import handlers
import logging
import datetime
from fin_traffic_data.scripts.fetch_raw_data import fetch_raw_data
from fin_traffic_data.scripts.aggregate_raw_data import aggregate_raw_data
from fin_traffic_data.scripts.get_aggregated_traffic_between_areas import get_aggregated_traffic_between_areas
from fin_traffic_data.scripts.export_area_data_as_csv import export_area_data_as_csv


def _parse_time_resolution(x):
    """Parse human-readable input of time resolution"""
    m = re.findall(r"(?P<num>\d+)(?P<qualif>w|d|h|m|s)", x)
    dt = datetime.timedelta()
    for obj in m:
        if obj[1] == 'w':
            dt += datetime.timedelta(days=int(obj[0])*7)
        elif obj[1] == 'd':
            dt += datetime.timedelta(days=int(obj[0]))
        elif obj[1] == 'h':
            dt += datetime.timedelta(hours=int(obj[0]))
        elif obj[1] == 'm':
            dt += datetime.timedelta(minutes=int(obj[0]))
        elif obj[1] == 's':
            dt += datetime.timedelta(seconds=int(obj[0]))
        else:
            raise ValueError(f"Unknown literal '{obj[1]}'")
    return dt


def determine_dates_to_fetch(logger, results_dir_fetch,
                             begin_date, end_date):
    logger.info('Determining dates to fetch TMS information')
    logger.debug(('Desired initial dates\n'
                  'Being date: %s.\n'
                  'End date: %s.') % (begin_date,
                                      end_date))
    match_pattern = '%s/fin_traffic_raw_*' % (results_dir_fetch, )
    aggregation_files = glob.glob(match_pattern)
    logger.debug('Looking for dates in files with pattern: %s' % (match_pattern))

    date_intervals = []
    if len(aggregation_files) == 0:
        logger.debug('No previous files were found. Going to use begin and end date')
        begin_first_interval = begin_date
        end_first_interval = end_date
        date_intervals.append((begin_first_interval, end_first_interval))
        logger.info(('First interval\n'
                     'Begin date: %s\n'
                     'End date: %s') % (begin_first_interval,
                                        end_first_interval))
    else:
        earliest_date = datetime.datetime(year=2050, month=1, day=1).date()
        latest_date = datetime.datetime(year=1970, month=1, day=1).date()
        for file in aggregation_files:
            logger.debug('Looking for dates in file: %s' % (file, ))
            regex_match = re.match((r".*fin_traffic_raw_.*(?P<begin_date>\d{4}-\d{2}-\d{2}).*"
                                    r"(?P<end_date>\d{4}-\d{2}-\d{2}).*"), file)
            if not regex_match:
                logger.debug('File did not have a begin or end date: %s' % (file, ))
                continue

            begin_date_file = datetime.datetime.strptime(regex_match.group('begin_date'),
                                                         "%Y-%m-%d").date()
            end_date_file = datetime.datetime.strptime(regex_match.group('end_date'),
                                                       "%Y-%m-%d").date()
            logger.debug('Begin date: %s. End date: %s.' % (begin_date_file, end_date_file))
            if begin_date_file < earliest_date:
                earliest_date = begin_date_file

            if end_date_file > latest_date:
                latest_date = end_date_file
        logger.debug(('Earliest date found: %s.\n'
                      'Latest date found: %s.') % (earliest_date,
                                                   latest_date))

        if ((begin_date < earliest_date and end_date <= earliest_date) or
           (begin_date >= latest_date and end_date > latest_date)):
            begin_first_interval = begin_date
            end_first_interval = end_date
            date_intervals.append((begin_first_interval, end_first_interval))
        elif begin_date < earliest_date and end_date <= latest_date:
            begin_first_interval = begin_date
            end_first_interval = earliest_date
            date_intervals.append((begin_first_interval, end_first_interval))
        elif begin_date >= earliest_date and end_date > latest_date:
            begin_first_interval = latest_date
            end_first_interval = end_date
            date_intervals.append((begin_first_interval, end_first_interval))
        elif begin_date < earliest_date and end_date > latest_date:
            begin_first_interval = begin_date
            end_first_interval = earliest_date
            date_intervals.append((begin_first_interval, end_first_interval))

            begin_second_interval = latest_date
            end_second_interval = end_date
            date_intervals.append((begin_second_interval, end_second_interval))

    logger.debug('Checking for large dates to do a batch of them')
    batched_intervals = []
    for begin_interval, end_interval in date_intervals:
        time_difference = end_interval - begin_interval
        if time_difference.days > 30:
            begin_batch_interval = begin_interval
            end_batch_interval = begin_interval + datetime.timedelta(days=30)
            while True:
                batched_intervals.append((begin_batch_interval, end_batch_interval))
                if end_batch_interval == end_interval:
                    break
                else:
                    begin_batch_interval = end_batch_interval
                    end_batch_interval = end_batch_interval + datetime.timedelta(days=30)
                    if end_batch_interval >= end_interval:
                        end_batch_interval = end_interval
        else:
            batched_intervals.append((begin_interval, end_interval))

    logger.info(('Determined intervals\n'
                 '%s') % (batched_intervals, ))

    return batched_intervals


def fetch_tms_data_aggregate(logger, begin_date, end_date,
                             progressbar_bool, results_dir_fetch,
                             time_resolution, results_dir_aggregate,
                             aggregation_level, visualize_bool,
                             results_dir_traffic):
    logger.info('Starting to fetch all data and aggregate.')
    date_intervals = determine_dates_to_fetch(logger=logger,
                                              results_dir_fetch=results_dir_fetch,
                                              begin_date=begin_date,
                                              end_date=end_date)
    logger.info('Date intervals determined.')

    if len(date_intervals) == 0:
        logger.info('No new date interval determined. Not doing anything.')
        return

    for begin_date_interval, end_date_interval in date_intervals:
        logger.info('Fetching raw data\n'
                    'Begin date: %s\n'
                    'End date: %s\n'
                    'Progressbar bool: %s\n'
                    'Results dir fetch: %s' % (begin_date_interval,
                                               end_date_interval,
                                               progressbar_bool,
                                               results_dir_fetch))
        results_dir_fetch = fetch_raw_data(begin_date=begin_date_interval,
                                           end_date=end_date_interval,
                                           progressbar_bool=progressbar_bool,
                                           results_dir=results_dir_fetch)
    logger.info('Raw data fetched!')

    logger.info('Aggregating data by time\n'
                'Aggregating raw data files in: %s\n'
                'Time resolution: %s\n'
                'Results dir time aggregated: %s' % (results_dir_fetch,
                                                     time_resolution,
                                                     results_dir_aggregate))
    results_dir_aggregate = aggregate_raw_data(basepath=results_dir_fetch,
                                               delta_t=time_resolution,
                                               results_dir=results_dir_aggregate)
    logger.info('Data aggregated by time!')

    # Getting the latest file that was aggregated.
    time_aggregated_files_match = glob.glob(results_dir_aggregate + "/*")
    latest_file_aggregate = max(time_aggregated_files_match, key=os.path.getctime)
    logger.info('Aggregating data by area\n'
                'Time aggregated input file: %s\n'
                'Aggregation level: %s\n'
                'Visaluzation enabled?: %s\n'
                'Results dir area aggregated: %s' % (latest_file_aggregate,
                                                     aggregation_level,
                                                     visualize_bool,
                                                     results_dir_traffic))
    result_path_traffic = get_aggregated_traffic_between_areas(inputfile=latest_file_aggregate,
                                                               area=aggregation_level,
                                                               visualization_enabled=visualize_bool,
                                                               results_dir=results_dir_traffic)
    logger.info('Data aggregated by area!')

    logger.info('Exporting results as CSV'
                'Exporting results in file: %s' % (result_path_traffic, ))
    result_tar_path = export_area_data_as_csv(inputpath=result_path_traffic)
    logger.info('Exported results in CSV!')

    logger.info('Finished to execute complete process of fetching TMS data')

    return result_tar_path


# Parse script arguments
def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(
        description=("Fetches raw data, aggregates it firs by time then by area"
                     "and finally it exports it as CSV.")
    )

    parser.add_argument('--begin-date',
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(),
                        help='Date from which to begin data acquisition',
                        required=True)

    parser.add_argument('--end-date',
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(),
                        help='Day past the last day of the data.',
                        required=True)

    parser.add_argument("--aggregation_level", "-al",
                        type=str,
                        default="hcd",
                        choices=["province", "erva", "hcd"],
                        help="Set the area aggregation level.")

    parser.add_argument("--time-resolution",
                        type=_parse_time_resolution,
                        required=True,
                        help="Time resolution of the aggregation")

    parser.add_argument("--results_dir_fetch", "-rf",
                        type=str,
                        default='raw_data',
                        help="Name of the directory to store the raw data results.")

    parser.add_argument("--results_dir_aggregate", "-ra",
                        type=str,
                        default='aggregated_data_time',
                        help="Name of the directory to store the aggregated data by time.")

    parser.add_argument("--results_dir_traffic", "-rt",
                        type=str,
                        default='aggregated_data_hcd',
                        help="Name of the directory to store the aggregated data by area.")

    # Arguments for logging
    parser.add_argument("--logfile", "-lf", type=str,
                        default="logs_complete_pipeline.log",
                        help="Name of the log file of the main process.")

    parser.add_argument("--loglevel", "-ll", type=str,
                        default="DEBUG",
                        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
                        help="Set logging level.")

    return parser.parse_args(args)


def main():
    start_time = time.time()
    args = parse_args()

    # Select data directory
    curr_dir = os.path.dirname(os.path.realpath(__file__))

    # Get a logger of the events
    logfile = os.path.join(curr_dir, args.logfile)
    numeric_log_level = getattr(logging, args.loglevel, None)
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %H:%M:%S %p',
        level=numeric_log_level,
        handlers=[
            # Max store 1 GB of logs
            handlers.RotatingFileHandler(logfile,
                                         maxBytes=100e6,
                                         backupCount=10),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger()
    logger.info('Logger ready. Logging to file: %s' % (logfile))
    try:
        fetch_tms_data_aggregate(logger=logger,
                                 begin_date=args.begin_date,
                                 end_date=args.end_date,
                                 progressbar_bool=False,
                                 results_dir_fetch=args.results_dir_fetch,
                                 time_resolution=args.time_resolution,
                                 results_dir_aggregate=args.results_dir_aggregate,
                                 aggregation_level=args.aggregation_level,
                                 visualize_bool=False,
                                 results_dir_traffic=args.results_dir_traffic)
    except Exception:
        logger.exception("Fatal error in main loop")
    finally:
        elapsed_time = time.time() - start_time
        elapsed_delta = datetime.timedelta(seconds=elapsed_time)
        logger.info('Total elapsed time of execution: %s' % (elapsed_delta, ))


if __name__ == "__main__":
    main()
