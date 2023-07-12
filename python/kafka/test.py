# TODO: not working:
# KeyError: 'beam:transform:org.apache.beam:kafka_read_with_metadata:v1'
"""
    Run the pipeline REMOTELY with either of the following:
        python basic_setup.py --requirements_file requirements.txt
        python basic_setup.py --setup_file setup.py
    If running locally, packages must be installed first.
"""
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka, default_io_expansion_service

def run(beam_options):
    with beam.Pipeline(options = beam_options) as pipeline:
        ret = (
        pipeline
        | "From Kafka" >> ReadFromKafka(
                consumer_config = {
                    "bootstrap.servers": "[::1]:9092"
                    ,"group.id": "testConsumer20230327"
                    ,"auto.offset.reset": "earliest"
                }
                ,topics = ["test"]
                ,with_metadata = True
                ,expansion_service = default_io_expansion_service()
            )
        | "Print" >> beam.Map(print)
        )


if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO)
    beam_options = PipelineOptions()
    opts = beam_options.get_all_options()
    logging.info(opts["streaming"])
    run(beam_options)
