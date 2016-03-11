# Hadoop_tfidf
Hadoop map reduce codes for calculating the term-frequency and inter-document frequency
Further the code helps to list the documents in order of relevance based on the search query.

The codes will help you calculate the term and inter-document frequencies of the words in the 10 input files.
Codes 1-4 help with this phase of claculation.
Codes 5-6 will subset the list of words to desired ones entered in the search query.

The output files are the outputs obtained after each stage.

P.S. - These codes can be run in Windows and Hadoop environment in the manner defined below:
-- Windows environment -- 
python G:\hadoop\stage1_mapper.py > G:\hadoop\new\ mapper1_out.txt
python G:\hadoop\remove_stopwords.py G:\hadoop\new\mapper1_out.txt > G:\hadoop\new\stop_out.txt
python G:\hadoop\stage_2.py G:\hadoop\new\stop_out.txt > G:\hadoop\new\stage2.txt
python G:\hadoop\stage_3.py G:\hadoop\new\stage2.txt > G:\hadoop\new\stage3_end.txt
python G:\hadoop\stage_4.py ads > G:\hadoop\new\stage4_end.txt
<input search word on prompt, example - dinner> 
python G:\hadoop\stage_5.py G:\hadoop\stage4_end.txt > G:\hadoop\new\dinner.txt
