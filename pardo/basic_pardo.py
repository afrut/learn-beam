#python basic_pardo.py
import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Transformation to be used by ParDo
class UppercaseFirst(beam.DoFn):
    def process(self, element):
        element = element[0].upper() + element[1:]
        yield element   # yield is used because the process() method is required to return an Iterable

def run():
    parser = argparse.ArgumentParser()
    known_args,  pipeline_args= parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options = pipeline_options) as p:
        _ = (p
            | "Create" >> beam.Create(["spam", "ham", "eggs", "toast", "bacon"])
            | beam.ParDo(UppercaseFirst())
            | beam.Map(logging.info)
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()