"""
    Run the pipeline with python basic.py
"""
import apache_beam as beam

def cap(s: str):
    """
        Upper-case the first letter
    """
    return s[0].upper() + s[1:]

if __name__ == "__main__":
    with beam.Pipeline() as pipeline:
        ret = (
            pipeline
            # Create a PCollection
            | "Create data" >> beam.Create(["spam", "ham", "eggs", "bacon", "toast"])

            # Apply a function to every element in the PCollection
            | "Upper-case first letter" >> beam.Map(cap)
            | "Print" >> beam.Map(print)
        )
