from unittest.mock import patch
import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from word_count import WordCount


class ArgsData:
        input = 'test'
        output = 'path'

class TestCount:
    @staticmethod
    @pytest.fixture()
    def test_data():
        test_data = ["THE BOY WHO LIVED",
        "Mr. and Mrs. Dursley, of number four, Privet Drive,",
        "were proud to say that they were perfectly normal,",
        "thank you very much. They were the last people you’d",
        "expect to be involved in anything strange or mysterious, because they just didn't hold with such",
        "nonsense."]
        return test_data

    @staticmethod
    def test_count_words(test_data):
        expected_output = [49]

        with TestPipeline() as pipeline:
            input_created = pipeline | beam.Create(test_data)
            output = input_created | WordCount.count_words()
            assert_that(output, equal_to(expected_output))

    @staticmethod
    def test_words_occurences(test_data):
        expected_output = ['THE: 1', 'BOY: 1', 'WHO: 1', 'LIVED: 1', 'Mr: 1', 'and: 1', 'Mrs: 1', 'Dursley: 1', 'of: 1', 'number: 1', 'four: 1', 'Privet: 1', 'Drive: 1', 'were: 3', 'proud: 1', 'to: 2', 'say: 1', 'that: 1', 'they: 2', 'perfectly: 1', 'normal: 1', 'thank: 1', 'you: 1', 'very: 1', 'much: 1', 'They: 1', 'the: 1', 'last: 1', 'people: 1', 'you’d: 1', 'expect: 1', 'be: 1', 'involved: 1', 'in: 1', 'anything: 1', 'strange: 1', 'or: 1', 'mysterious: 1', 'because: 1', 'just: 1', "didn't: 1", 'hold: 1', 'with: 1', 'such: 1', 'nonsense: 1']

        with TestPipeline() as pipeline:
            input_created = pipeline | beam.Create(test_data)
            output = input_created | WordCount.words_occurences()
            assert_that(output, equal_to(expected_output))
        
    @staticmethod
    @patch('count.PipelineOptions')
    @patch('count.beam')
    def test_run_pipeline(mock_pipeline, mock_options):
        real = WordCount.run_pipeline(ArgsData, "path")
        assert (real is None)

    
    @staticmethod
    def test_filter_by_word_frequency(test_data):
        expected_output = ['were: 3', 'to: 2', 'they: 2']

        with TestPipeline() as pipeline:
            input_created = pipeline | beam.Create(test_data)
            output = input_created | WordCount.filter_by_word_frequency(1)
            assert_that(output, equal_to(expected_output))