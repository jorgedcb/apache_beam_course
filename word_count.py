import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re

find_word_regex = r'[A-Za-z\’\']+'

class WordCount:
    
    @staticmethod
    @beam.ptransform_fn
    def count_words(input_data):
        return (
        input_data
        | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(find_word_regex, x))
        | 'Count all elements' >> beam.combiners.Count.Globally()
        )

    @staticmethod
    @beam.ptransform_fn
    def words_occurences(input_data):
        return (
        input_data
        | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(find_word_regex, x))
        | beam.combiners.Count.PerElement()
        | beam.MapTuple(lambda word, count: '%s: %s' % (word, count))
        )

    @staticmethod
    @beam.ptransform_fn
    def filter_by_word_frequency(input_data, frequency):
        return (
        input_data
        | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(find_word_regex, x))
        | beam.combiners.Count.PerElement()
        | beam.Filter(lambda x : x[1]>frequency)
        | beam.MapTuple(lambda word, count: '%s: %s' % (word, count))
        )
    
    @classmethod
    def run_pipeline(cls, our_args, beam_args):
        inputs_pattern = our_args.input
        outputs_pattern = our_args.output
        opts = PipelineOptions(beam_args)
        with beam.Pipeline(options=opts) as pipe:
            data = pipe | "Read historical income data" >> beam.io.ReadFromText(inputs_pattern)
            data | cls.count_words() | beam.Map(print)
            _ = (data 
                | cls.words_occurences() 
                | "write the output of words_occurences.txt" >> beam.io.WriteToText(
                    file_path_prefix=outputs_pattern,
                    shard_name_template="",
                    file_name_suffix="output_words_occurences.txt",
                ))
            _ = (data 
                | cls.filter_by_word_frequency(1000) 
                | "write the output of filter_by_word_frequency.txt" >> beam.io.WriteToText(
                    file_path_prefix=outputs_pattern,
                    shard_name_template="",
                    file_name_suffix="output_filter_by_word_frequency.txt",
                ))