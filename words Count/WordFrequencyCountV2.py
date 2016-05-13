from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile(r"[\w']+")

class WordFrequencyCountV2(MRJob):
    def mapper(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1
            
        
    def reducer(self, key, values):
        yield key, sum(values)
        
if __name__ == '__main__':
    WordFrequencyCountV2.run()