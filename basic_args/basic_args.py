#python basic_args.py
#python basic_args.py --input input2.txt --output output2.txt
#python basic_args.py --input input2.txt --output output2.txt --DirectRunner

# Standard imports
import argparse
import logging

# Beam imports
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

# Transformation functions
import re


def uppercase_first(s: str):
    """
        Upper-case the first letter
    """
    return s[0].upper() + s[1:]

def find_words(s: str):
    """
        Returns a list of words from the string
    """
    return re.findall(r"[A-Za-z\']+", s)

def run():
    # Developer-defined arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='./input.txt')
    parser.add_argument(
        '--output',
        dest='output',
        default='./output.txt')
    known_args, pipeline_args = parser.parse_known_args()

    # Unknown args are assumend to be for the pipeline
    logging.info(f"pipeline_args = {pipeline_args}")

    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=pipeline_options) as p:
        # _ because we don't need to reference the PCollection
        _ = (p
            | ReadFromText(known_args.input)    # 1 element of PCollection = 1 line
            # | beam.Map(find_words)            # 1 element of PCollection = list[str]
            | beam.FlatMap(find_words)          # 1 element of PCollection = str
            | beam.Map(uppercase_first)         # FlatMap will apply 
            | WriteToText(known_args.output))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()