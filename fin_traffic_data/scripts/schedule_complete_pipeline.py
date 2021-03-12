import os
import sys
import time
import argparse
import re
from logging import handlers
import logging
import datetime
from fin_traffic_data.scripts.complete_pipeline import fetch_tms_data_aggregate


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


def schedule_complete_pipeline(logger, begin_date, results_dir_fetch,
                               time_resolution, results_dir_aggregate,
                               aggregation_level, results_dir_traffic):
    while True:
        now_time = datetime.datetime.now()
        logger.info('Current time: %s' % (now_time, ))
        if now_time.hour >= 12:
            start_execution = time.time()
            fetch_tms_data_aggregate(logger=logger,
                                     begin_date=begin_date,
                                     end_date=now_time.date(),
                                     progressbar_bool=False,
                                     results_dir_fetch=results_dir_fetch,
                                     time_resolution=time_resolution,
                                     results_dir_aggregate=results_dir_aggregate,
                                     aggregation_level=aggregation_level,
                                     visualize_bool=False,
                                     results_dir_traffic=results_dir_traffic)
            elapsed_execution = start_execution - time.time()
            elapsed_delta = datetime.timedelta(seconds=elapsed_execution)
            logger.info(('Sleeping for 1 hour.'
                         ' Total elapsed time of execution: %s') % (elapsed_delta, ))
            time.sleep(3600)
        else:
            logger.info('Still not 12 pm. Sleeping for 1 hour.')
            time.sleep(3600)


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
                        default='aggregated_data_area',
                        help="Name of the directory to store the aggregated data by area.")

    # Arguments for logging
    parser.add_argument("--logfile", "-lf", type=str,
                        default="logs_schedule_complete_pipeline.log",
                        help="Name of the log file of the main process.")

    parser.add_argument("--loglevel", "-ll", type=str,
                        default="INFO",
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
        schedule_complete_pipeline(logger=logger,
                                   begin_date=args.begin_date,
                                   results_dir_fetch=args.results_dir_fetch,
                                   time_resolution=args.time_resolution,
                                   results_dir_aggregate=args.results_dir_aggregate,
                                   aggregation_level=args.aggregation_level,
                                   results_dir_traffic=args.results_dir_traffic)
    except Exception:
        logger.exception("Fatal error in main loop")
    finally:
        elapsed_time = time.time() - start_time
        elapsed_delta = datetime.timedelta(seconds=elapsed_time)
        logger.info('Total elapsed time of execution: %s' % (elapsed_delta, ))


if __name__ == "__main__":
    main()
