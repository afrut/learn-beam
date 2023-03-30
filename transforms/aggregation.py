#python group_by_key.py
"""
    A playarea for GroupByKey
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
        | "Create data" >> beam.Create([
                (190917, 164)
                ,(194085, 194)
                ,(113975, 121)
                ,(115375, 189)
                ,(166145, 113)
                ,(161234, 148)
                ,(196261, 195)
                ,(102051, 114)
                ,(102949, 147)
                ,(160151, 101)
                ,(190917, 123)
                ,(194085, 150)
                ,(113975, 163)
                ,(115375, 142)
                ,(166145, 162)
                ,(161234, 132)
                ,(196261, 133)
                ,(102051, 164)
                ,(102949, 181)
                ,(160151, 157)
                ,(190917, 163)
                ,(194085, 192)
                ,(113975, 191)
                ,(115375, 199)
                ,(166145, 151)
                ,(161234, 159)
                ,(196261, 124)
                ,(102051, 109)
                ,(102949, 183)
                ,(160151, 106)
                ,(190917, 138)
                ,(194085, 157)
                ,(113975, 195)
                ,(115375, 193)
                ,(166145, 137)
                ,(161234, 196)
                ,(196261, 152)
                ,(102051, 109)
                ,(102949, 161)
                ,(160151, 137)
            ])
        )

        # Aggregate every element by key.
        # Go from:
        # (key1, val1)
        # (key1, val2)
        # (key1, val3)
        # to (unique_key, [val1, val2, val3])
        group_by_key = (data
        | "GroupByKey" >> beam.GroupByKey()
        | "Log GroupByKey" >> beam.Map(logging.info)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

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