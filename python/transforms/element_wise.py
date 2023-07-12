#python keys.py
"""
    A playarea for key-related transforms
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
        data_list = (p
        | "Create list data" >> beam.Create([
                (1, ["spam", "juice"])          # Data has to be in a 2-tuple to use keys
                ,(2, ["ham", "coffee"])         # The first element is always the key
                ,(1, ["eggs", "coffee"])
                ,(1, ["bacon", "coffee"])
                ,(2, ["toast", "juice"])
                ,(3, ["fruit", "juice"])
                ,(4, ["pancakes", "coffee"])
                ,(1, ["homefries", "coffee"])
            ])

        # Executes a function for each element
        | "Add data to each element" >> beam.Map(lambda x: (x[0], x[1] + ["complimentary bread"]))
        )
        # A data_list element now looks like (key, [str, str, str])

        data_str = (p
        | "Create string data" >> beam.Create([
                "To Sherlock Holmes she is always the woman. I have seldom heard him"
                ,"mention her under any other name. In his eyes she eclipses and"
                ,"predominates the whole of her sex. It was not that he felt any"
                ,"emotion akin to love for Irene Adler. All emotions, and that one"
                ,"particularly, were abhorrent to his cold, precise but admirably"
                ,"balanced mind. He was, I take it, the most perfect reasoning and"
                ,"observing machine that the world has seen, but as a lover he would"
                ,"have placed himself in a false position. He never spoke of the softer"
                ,"passions, save with a gibe and a sneer. They were admirable things"
                ,"for the observer--excellent for drawing the veil from men's motives"
                ,"and actions. But for the trained reasoner to admit such intrusions"
                ,"into his own delicate and finely adjusted temperament was to"
                ,"introduce a distracting factor which might throw a doubt upon all his"
                ,"mental results. Grit in a sensitive instrument, or a crack in one of"
                ,"his own high-power lenses, would not be more disturbing than a strong"
                ,"emotion in a nature such as his. And yet there was but one woman to"
                ,"him, and that woman was the late Irene Adler, of dubious and"
                ,"questionable memory."
            ])
        )

        # Every line is divided into words. Every word becomes an element in the PCollection.
        _ = (data_str
        | "Line to words" >> beam.FlatMap(lambda x: x.split(" "))
        | "Count words" >> beam.combiners.Count.Globally()
        | "Log word count" >> beam.Map(logging.info)
        )

        # Log all the keys
        _ = (data_list
        | "Get keys" >> beam.Keys()
        | "Log keys" >> beam.Map(logging.info)
        )

        # Log all the values
        _ = (data_list
        | "Get values" >> beam.Values()
        | "Log values" >> beam.Map(logging.info)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()

    logging.info(f"known_args = {known_args}")
    logging.info(f"pipeline_args = {pipeline_args}")
    run(known_args, pipeline_args)