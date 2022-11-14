import argparse
from word_count import WordCount

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input file to process.')
    
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')

    path_args, pipeline_args = parser.parse_known_args()
    WordCount.run_pipeline(path_args, pipeline_args)
   
if __name__ == '__main__':
    main()