"""
    A playarea for side inputs
"""
import argparse
import logging

import apache_beam as beam
import apache_beam.pvalue as pvalue
from apache_beam.options.pipeline_options import PipelineOptions

def upper_first(element: str, end: str, num: int):
    ret = element[0].upper() + element[1:]
    for _ in range(num):
        ret = ret + end
    return ret

class MyTransform(beam.DoFn):
    def process(self, element: str, end: str):
        yield element[0:-1] + element[-1].upper() + end

def run(known_args: dict, pipeline_args: dict):
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options = pipeline_options) as p:
        data = (p
            | "Create data" >> beam.Create(["spam", "ham", "eggs", "bacon", "toast"]))
        min_word_length = (data
            | "Length of each word" >> beam.Map(len)
            | "Min of Length" >> beam.CombineGlobally(lambda x: min(x)))
        transformed = (data
            | "Transform" >> beam.Map(upper_first
                ,end = "!"                                      # Pass a hard-coded parameter as side input
                ,num = pvalue.AsSingleton(min_word_length))     # Pass a PCollection as side input
        )
        transformed_pardo = (data
            | "Transform ParDo" >>
                beam.ParDo(MyTransform(), end = "?"))           # Can also pass side inputs to ParDo
        _ = transformed | beam.Map(lambda x: logging.info(f"transformed {x}"))
        _ = transformed_pardo | beam.Map(lambda x: logging.info(f"transformed_pardo {x}"))

if __name__ == '__main__':
    fmt = "%(asctime)s.%(msecs)03d: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format = fmt, level = logging.INFO, datefmt = datefmt)

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()

    logging.info(f"known_args = {known_args}")
    logging.info(f"pipeline_args = {pipeline_args}")
    run(known_args, pipeline_args)