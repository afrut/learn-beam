#python periodic_impulse.py \
#    --interval 60 \
#    --interval 10
"""
    A playarea for the PeriodicImpulse transform
"""
import argparse
import logging

import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.periodicsequence import PeriodicImpulse

def run(known_args: dict, pipeline_args: dict):
    """
        Main pipeline
    """
    pipeline_options = PipelineOptions(pipeline_args)

    now = datetime.datetime.now()
    start_time = datetime.datetime.timestamp(now)
    end_time = datetime.datetime.timestamp(
        now + datetime.timedelta(seconds = known_args.total_seconds)
    )
    with beam.Pipeline(options = pipeline_options) as p:
        data = (p
        | PeriodicImpulse(                              # Periodically generate an integer
                start_timestamp = start_time            # starting this timestamp
                ,stop_timestamp = end_time              # stopping at this timestamp
                ,fire_interval = known_args.interval    # every this many seconds
            )
        | beam.Map(logging.info)
        )
        
if __name__ == '__main__':
    fmt = "%(asctime)s.%(msecs)03d: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--interval'
        ,type = int
        ,default = 6
        ,help = "Number of seconds in between firing")
    parser.add_argument(
        '--total_seconds'
        ,type = int
        ,default = 60
        ,help = "Number of total seconds to fire")
    known_args, pipeline_args = parser.parse_known_args()

    logging.info(f"known_args = {known_args}")
    logging.info(f"pipeline_args = {pipeline_args}")
    run(known_args, pipeline_args)