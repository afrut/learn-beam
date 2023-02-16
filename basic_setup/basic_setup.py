"""
    Run the pipeline REMOTELY with either of the following:
        python basic_setup.py --requirements_file requirements.txt
        python basic_setup.py --setup_file setup.py
    If running locally, packages must be installed first.
"""
import logging
import apache_beam as beam
import my_package.func as func
from apache_beam.options.pipeline_options import PipelineOptions

if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO)
    beam_options = PipelineOptions()
    opts = beam_options.get_all_options()
    logging.info(opts["requirements_file"])
    logging.info(opts["setup_file"])
    with beam.Pipeline(options = beam_options) as pipeline:
        ret = (
            pipeline
            # Create a PCollection
            | "Create data" >> beam.Create(["spam", "ham", "eggs", "bacon", "toast"])

            # Apply a function to every element in the PCollection
            | "Upper-case first letter" >> beam.Map(func.cap)
            | "Print" >> beam.Map(print)
        )