from mrjob.job import MRJob
from mrjob.step import MRStep

import math


class MRWordCount(MRJob):

    ## Mapper to input the key, value pairs remaining after stopwords
    def stage2_mapper(self,_,line):
        word, count = line.split('\t')
        count = str(count) + "_" + word.split('_')[1]
        word = word.replace('\\','')
        word = word.replace('"','')
        word = word.replace('\'','')

        count = count.replace('\\','')
        count = count.replace('"','')

        yield word.split('_')[0], count


    def stage2_reducer(self,word,count):
        total_documents = 10
        doc_freq_map = []
        dist_file_id_list = []

        ## Count distinct documents in which the word appears
        for term_freq in count:
            doc_freq_map.append(str(term_freq))
            dist_file_id_list.append(str(term_freq).split('_')[1])

        dist_file_id_list = set(dist_file_id_list)


        word = word.replace('\\','')
        word = word.replace('"','')

        ## Calculate the term frequency and inter-document frequency
        for doc_freq_map_iter in doc_freq_map:
            word_file = word + '_'+ doc_freq_map_iter.split('_')[1]
            term_freq = int(doc_freq_map_iter.split('_')[0])
            tfidf = (1 + math.log(term_freq))*(math.log(total_documents/len(dist_file_id_list)))
            yield word_file, tfidf

    def steps(self):
        return [
                MRStep(mapper=self.stage2_mapper,
                       reducer = self.stage2_reducer,
                       )
                ]
if __name__ == '__main__':
    MRWordCount.run()
