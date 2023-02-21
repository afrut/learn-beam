#python composite_transforms.py
"""
    Create a composite transform
"""
import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def uppercase_first(x: str):
    return x[0].upper() + x[1:]

def uppercase_last(x:str):
    return x[:-1] + x[-1].upper()

# Define a composite transform
@beam.ptransform_fn
def CompositeUpper(pcoll):
    # Input is a PCollection. Output is a PCollection
    return (
        pcoll
        | "Uppercase First" >> beam.Map(uppercase_first)
        | "Uppercase Last" >> beam.Map(uppercase_last)
    )

def run():
    with beam.Pipeline() as p:
        _ = (p
            | "Create data" >> beam.Create(["spam","ham","eggs","toast","bacon"])
            | "Composite Upper" >> CompositeUpper()
            | "Print" >> beam.Map(print)
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()