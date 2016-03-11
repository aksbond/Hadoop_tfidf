from mrjob.job import MRJob
from mrjob.step import MRStep
import operator
output = []


class MRWordCount(MRJob):

    ## Mapper
    def stage5_mapper(self,_,line):
        line = line.replace('\\','')
        line = line.replace('"','')
        line = line.strip()

        word_file_id, norm_tfidf = line.split("\t")
        file_id = word_file_id.split('_')[1]
        norm_tfidf = float(norm_tfidf)

        yield file_id, norm_tfidf

    ## Reducer calculates the tf-idf for each document
    def add_norm_tfidf(self,file_id, norm_tfidfs):
        output.append((sum(norm_tfidfs), file_id))

    ## Reducer sorts the documents in decreasing order of tf-idf
    def sort_norm_tfidf(self):
        sorted_words = sorted(output, key=operator.itemgetter(0), reverse = True)
        for normidf,file_id in sorted_words[:]:
            yield file_id, normidf

    def steps(self):
        return [
                MRStep(mapper=self.stage5_mapper,
                       reducer = self.add_norm_tfidf,
                       reducer_final = self.sort_norm_tfidf,
                       )
                ]
if __name__ == '__main__':
    MRWordCount.run()
