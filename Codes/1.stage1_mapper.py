#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""
from os import listdir
from os.path import isfile, join
import re

# mapper function
def read_input(file, file_ind):
    file_handle = open(file, 'r')
    for line in file_handle.readlines():
        for word in line.split():
                ## Remove punctuations from the words
                regex = re.compile('[%s]' % re.escape('!"#$%&()*+,./:;<=>?@[\\]^_`{|}~'))
                word = regex.sub('', word)
                ## Convert all words to lower case for better matching
                word = word.lower()
                if word == "":
                    continue
                else:
                    yield(word + file_ind)

def main(separator='\t'):
    # input comes from STDIN (standard input)
    stopword_file = "G:/hadoop/input_files/stopwords.txt"

    data = read_input(stopword_file, '_stop')
    for word_file_id in data:
        print(word_file_id, separator, 1)

    # 10 input files
    mypath = "G:/hadoop/input_files/"
    onlyfiles = [f for f in listdir(mypath) if ((isfile(join(mypath, f))) & (f != 'stopwords.txt')) ]
    file_cntr = 0
    for file in onlyfiles:
        file_cntr = file_cntr + 1
        data = read_input(mypath + file, "_f"+str(file_cntr))
        for word_file_id in data:
            print(word_file_id, separator, 1)

if __name__ == "__main__":
    main()
