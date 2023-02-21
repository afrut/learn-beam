#python multiple_outputs.py
"""
    Demonstrates a ParDo with multiple outputs
"""
import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue

# Transformation to be used by ParDo
class UppercaseFirst(beam.DoFn):
    # Tags used to reference multiple outputs
    OUTPUT_TAG_STARTS_WITH_VOWEL = 'tag_starts_with_vowel'
    OUTPUT_TAG_STARTS_WITH_CONSONANT = 'tag_starts_with_consonant'

    def process(self, element):
        if element[0] in 'aeiou':
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_STARTS_WITH_VOWEL, element)
        else:
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_STARTS_WITH_CONSONANT, element)
        yield element[0].upper() + element[1:] # yield is used because the process() method is required to return an Iterable

def run():
    parser = argparse.ArgumentParser()
    known_args,  pipeline_args= parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options = pipeline_options) as p:
        outputs = (p
            | "Create" >> beam.Create(["spam", "ham", "eggs", "toast", "bacon", "apple", "orange"])
            | beam.ParDo(UppercaseFirst()).with_outputs(
                UppercaseFirst.OUTPUT_TAG_STARTS_WITH_VOWEL
                ,UppercaseFirst.OUTPUT_TAG_STARTS_WITH_CONSONANT
                ,main = "uppered"   # Define tag for main output
            )
        )
        # outputs is of type DoOutputsTuple
        uppered, _, _ = outputs                                         # access like a tuple
        vowels = outputs[UppercaseFirst.OUTPUT_TAG_STARTS_WITH_VOWEL]   # access like a dict
        consonants = outputs.tag_starts_with_consonant                  # access like a field in an object


        (vowels | "Log Vowels" >> beam.Map(lambda x: logging.info(f"vowel = {x}")))
        (consonants | "Log Consonants" >> beam.Map(lambda x: logging.info(f"consonant = {x}")))
        (uppered | "Log Uppercased" >> beam.Map(lambda x: logging.info(f"uppercased = {x}")))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()