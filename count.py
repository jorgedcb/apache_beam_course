import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re
class Count:

    @staticmethod
    @beam.ptransform_fn
    def words_occurences(input_data):
         return (
            input_data
            | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
            | beam.combiners.Count.PerElement()
            | beam.MapTuple(lambda word, count: '%s: %s' % (word, count))
         )

    @staticmethod
    @beam.ptransform_fn
    def count_words(input_data):
         return (
            input_data
            | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
            | 'Count all elements' >> beam.combiners.Count.Globally()
         )
        
    @classmethod
    def run_pipeline(cls, our_args, beam_args):
        inputs_pattern = our_args.input
        outputs_pattern = our_args.output
        opts = PipelineOptions(beam_args)
        with beam.Pipeline(options=opts) as pipe:
            data = pipe | "Read historical income data" >> beam.io.ReadFromText(inputs_pattern)
            _ = (data 
                | cls.words_occurences() 
                | beam.io.WriteToText(
                    file_path_prefix=outputs_pattern,
                    shard_name_template="",
                    file_name_suffix="output.txt",
                ))
            data | cls.count_words() | beam.Map(print)