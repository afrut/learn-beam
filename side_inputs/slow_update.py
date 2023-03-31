#python main.py --DirectRunner --input_file="../../txt/input.txt"
#TODO: not working
"""
    A playarea for text-based input
"""
import argparse
import logging
import datetime

import math
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms import window

class GetWindowData(beam.DoFn):
    def process(self, element
        ,timestamp = beam.DoFn.TimestampParam
        ,window = beam.DoFn.WindowParam):
        yield (element, timestamp, (window.start,window.end))

def cross_join(left, rights):
    for x in rights:
      yield (left, x)

def run(known_args: dict, pipeline_args: dict):
    """
        Main pipeline
    """
    pipeline_options = PipelineOptions(pipeline_args)
    logging.info(pipeline_options)
    now = datetime.datetime.timestamp(datetime.datetime.now())

    # first_ts = math.floor(time.time()) - 30
    first_ts = math.floor(time.time())
    last_ts = first_ts + 45
    interval = 5
    main_input_windowing_interval = 7

    # aligning timestamp to get persistent results
    first_ts = first_ts - (
        first_ts % (interval * main_input_windowing_interval))
    last_ts = first_ts + 45
    logging.info(f"first_ts = {first_ts}")

    sample_main_input_elements = [
        first_ts - 2, # no output due to no SI
        first_ts + 1,  # First window
        first_ts + 8,  # Second window
        first_ts + 15,  # Third window
        first_ts + 22,  # Fourth window
    ]
    src_file_pattern = "../../txt/input02.txt"

    for x in sample_main_input_elements:
        logging.info(f"{x}: {datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')}")

    with beam.Pipeline(options = pipeline_options) as p:
        impulse = (p
            | "PeriodicImpulse" >> PeriodicImpulse(
                start_timestamp = first_ts
                ,stop_timestamp = last_ts
                ,fire_interval = interval
                ,apply_windowing = True))

        side_input = (impulse
            | "MapToFileName" >> beam.Map(lambda x: src_file_pattern)
            | "ReadFromFile" >> beam.io.ReadAllFromText())

        main_input = (p
            | beam.Create(sample_main_input_elements)
            | beam.Map(lambda x: TimestampedValue(x, x))
            | beam.WindowInto(window.FixedWindows(main_input_windowing_interval)))

        crossed = (main_input
            | beam.FlatMap(cross_join, rights = beam.pvalue.AsIter(side_input)))

        _ = (crossed
            | beam.ParDo(GetWindowData())
            | beam.Map(logging.info))

if __name__ == "__main__":
    fmt = "%(asctime)s.%(msecs)03d: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_file"
        ,type = str
        ,required = True
        ,help = "File to pass as input")
    known_args, pipeline_args = parser.parse_known_args()

    logging.info(f"known_args = {known_args}")
    logging.info(f"pipeline_args = {pipeline_args}")
    run(known_args, pipeline_args)