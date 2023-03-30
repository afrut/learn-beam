"""
    Using ReadAllFromText.
"""
import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run(known_args: dict, pipeline_args: dict):
    num_files = known_args.num_files
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options = pipeline_options) as p:
        data = (p
            | "Start" >> beam.Create([x for x in range(num_files)])
            | "GetFilenames" >> beam.Map(lambda x: f"../../txt/input0{x}.txt")
            | "ReadAllTextFromFile" >> beam.io.ReadAllFromText())
        _ = data | beam.Map(logging.info)

if __name__ == '__main__':
    fmt = "%(asctime)s.%(msecs)03d: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--num_files'
        ,type = int
        ,default = 3
        ,help = "A non-pipeline parameter")
    known_args, pipeline_args = parser.parse_known_args()

    logging.info(f"known_args = {known_args}")
    logging.info(f"pipeline_args = {pipeline_args}")
    run(known_args, pipeline_args)