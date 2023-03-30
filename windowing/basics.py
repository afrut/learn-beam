#python basics.py --DirectRunner --num_keys 1 --num_rows 600 --seconds_into_future 60 --fixed_windows_length 20
"""
    A playarea for windowing
"""
import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window

import random
import hashlib
import datetime
import pytz
from typing import List, Tuple, Dict

def generate_keys(num_keys: int = 10) -> List:
    """
        Generate some 6-digit ints
    """
    return [random.randint(100000, 999999) for _ in range(num_keys)]

def generate_data(keys: List[int]
    ,num_rows: int
    ,seconds_into_future: int = 120
) -> List[Tuple[int, Dict]]:
    """
        Generate data by hashing the time now less the value of the key in seconds
    """
    now = datetime.datetime.now()
    def generate_timestamp() -> str:
        """
            Simulate an event time
        """
        delta = datetime.timedelta(seconds = random.randint(0, seconds_into_future))
        return (now + delta).strftime("%Y-%m-%d %H:%M:%S.%f")

    def generate_hash(key):
        """
            Generate some data based on key and time
        """
        return hashlib.sha256(
                str(now - datetime.timedelta(seconds = key))\
                .encode("utf-8")
            ).hexdigest()

    N = len(keys)
    data = []
    for _ in range(num_rows):
        key = keys[random.randint(0, N - 1)]
        data.append((
            key
            ,{
                "timestamp": generate_timestamp()
                ,"data": {
                    "amount": random.randint(1, 100)
                    ,"unique_string": generate_hash(key)
                }
            } 
        ))
    return data

def get_timestamp_from_data(element: Tuple[int, Dict]):
    return datetime.datetime.timestamp(
        datetime.datetime.strptime(element[1]["timestamp"], "%Y-%m-%d %H:%M:%S.%f")
    )

class GetTimestamp(beam.DoFn):
  def process(self, x, timestamp = beam.DoFn.TimestampParam):
    yield "{} - {}".format(timestamp.to_utc_datetime(), x[0])

class CountElements(beam.CombineFn):
    def create_accumulator(self):
        return 0
    
    def add_input(self, accumulator, input):
        return accumulator + 1
    
    def merge_accumulators(self, accumulators):
        return sum(accumulators)
    
    def extract_output(self, accumulator):
        return accumulator

class GetWindowData(beam.DoFn):
    def process(self, element, window = beam.DoFn.WindowParam):
        yield (element
            ,(
                window.start.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S.%f")
                ,window.end.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S.%f")
            )
        )

def run(known_args: dict, pipeline_args: dict):
    """
        Main pipeline
    """
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options = pipeline_options) as p:

        data = (p
        # Generate some data

        | "Create data" >> beam.Create(
            generate_data(generate_keys(known_args.num_keys)
                ,known_args.num_rows
                ,known_args.seconds_into_future ))

        # Timestamp each element based on data in the payload
        | "Assign timestamp" >> beam.Map(lambda x: beam.window.TimestampedValue(x
                ,get_timestamp_from_data(x))
            )
        )

        # # Get timestamp of every element
        # _ = (data
        # | "Get timestamp" >> beam.ParDo(GetTimestamp())
        # | beam.Map(lambda x: logging.info(f"type: {type(x)}, value: {x}"))
        # )

        # Apply fixed windows and count the number of elements in the current window
        _ = (data
        | "Apply fixed windows" >> beam.WindowInto(window.FixedWindows(known_args.fixed_windows_length))
        | "Count elements per window" >> beam.CombineGlobally(CountElements()).without_defaults()
        | "Add window data" >> beam.ParDo(GetWindowData())
        | "Log counts" >> beam.Map(logging.info)
        )

if __name__ == "__main__":
    fmt = "%(asctime)s.%(msecs)03d: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)
    random.seed(0)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num_keys"
        ,type = int
        ,default = 20
        ,help = "Number of keys to generate"
    )
    parser.add_argument(
        "--num_rows"
        ,type = int
        ,default = 100
        ,help = "Number of total elements to generate")
    parser.add_argument(
        "--seconds_into_future"
        ,type = int
        ,default = 120
        ,help = "Maximum value of timestamp defined by the number of seconds from now")
    parser.add_argument(
        "--fixed_windows_length"
        ,type = int
        ,default = 20
        ,help = "Maximum value of timestamp defined by the number of seconds from now")
    
    known_args, pipeline_args = parser.parse_known_args()

    logging.info(f"known_args = {known_args}")
    logging.info(f"pipeline_args = {pipeline_args}")
    run(known_args, pipeline_args)