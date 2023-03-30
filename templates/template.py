"""
    A general to start from when developing Beam pipelines
"""
import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run(known_args: dict, pipeline_args: dict):
    """
        Main pipeline
    """
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options = pipeline_options) as p:
        data = (p
        | "Create data" >> beam.Create(["spam", "ham", "eggs", "bacon", "toast"])
        )
        _ = data | beam.Map(logging.info)

if __name__ == '__main__':
    fmt = "%(asctime)s.%(msecs)03d: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input'
        ,type = int
        ,default = 6
        ,help = "A non-pipeline parameter")
    known_args, pipeline_args = parser.parse_known_args()

    logging.info(f"known_args = {known_args}")
    logging.info(f"pipeline_args = {pipeline_args}")
    run(known_args, pipeline_args)