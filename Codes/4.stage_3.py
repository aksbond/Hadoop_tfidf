from mrjob.job import MRJob
from mrjob.step import MRStep

import math

class MRWordCount(MRJob):

    ## Mapper to input the words and their respective tf-idfs
    def stage3_mapper(self,_,line):

        line = line.replace('\\','')
        line = line.replace('"','')

        word_file, tfidf = line.split("\t")
        word = word_file.split('_')[0]
        file_id = word_file.split('_')[1]

        yield file_id, word + '_' + tfidf

    ## This reducer calculates the normalized tf-idfs
    def stage3_reducer(self,file_id,word_tfidfs):
        total_tfidf = 0
        word_tfidf_list = []

        for word_tfidf in word_tfidfs:
            word_tfidf_list.append(word_tfidf)
            total_tfidf  = total_tfidf + (float(word_tfidf.split('_')[1]) * float(word_tfidf.split('_')[1]))

        file_id = file_id.replace('\\','')
        file_id = file_id.replace('"','')

        ## Normalization
        for word_tfidf in word_tfidf_list:
            word_file = word_tfidf.split('_')[0] + '_' + file_id
            tfidf = float(word_tfidf.split('_')[1])
            tfidf = tfidf/math.sqrt(total_tfidf)
            yield word_file, tfidf

    def steps(self):
        return [
                MRStep(mapper=self.stage3_mapper,
                       reducer = self.stage3_reducer,
                       )
                ]
if __name__ == '__main__':
    MRWordCount.run()
