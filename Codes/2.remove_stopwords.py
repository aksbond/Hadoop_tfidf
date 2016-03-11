from mrjob.job import MRJob
from mrjob.step import MRStep

import math

class MRWordCount(MRJob):

    ## Mapper remove stopwords from the list of words
    def stopword_mapper(self,_,line):

        line = line.replace('\\','')
        line = line.replace('"','')

        word_file_id, count = line.split("\t")
        word = word_file_id.split('_')[0]
        file_id = word_file_id.split('_')[1]

        file_id = file_id.strip()
        count = count.strip()

        file_id_count = file_id + '_' + count

        yield word, file_id_count

    ## Reducer to count the frequency of each word in the file
    def stopword_reducer(self,word,file_id_counts):

        word = word.replace('\\','')
        word = word.replace('"','')

        temp_store_list = []
        for file_id_count in file_id_counts:
            if file_id_count.split('_')[0] == 'stop':
                temp_store_list = []
                break
            else:
                word_file_id = word + '_' + file_id_count.split('_')[0]
                temp_store_list.append(word_file_id + '#' + file_id_count.split('_')[1])

        for item in temp_store_list:
            yield item.split('#')[0], int(item.split('#')[1]),

    def final_reducer(self,word_file_id, count):
        yield word_file_id, sum(count)


    def steps(self):
        return [
                MRStep(mapper=self.stopword_mapper,
                       combiner = self.stopword_reducer,
                       reducer = self.final_reducer
                       )
                ]
if __name__ == '__main__':
    MRWordCount.run()
