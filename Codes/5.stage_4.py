import sys

## Mapper to keep only search words in the list
def read_input(file,search_words):
    file_handle = open(file, 'r')
    for line in file_handle.readlines():
    # # split the line into words
        line.strip()
        line = line.replace('\\','')
        line = line.replace('"','')
        accept_flag = 0
        word_file_id, norm_tfidf = line.split('\t')

        for search_word in search_words.split(' '):
            search_word = search_word.strip()
            if word_file_id.split("_")[0] == search_word:
                accept_flag = 1
                break
        if accept_flag == 1:
            yield line.strip()

def main(separator='\t'):
    # input comes from STDIN (standard input)
    search_words = sys.stdin.readline()
    data = read_input(file = 'G:/hadoop/new/stage3_end.txt', search_words = search_words)
    for line in data:
        print(line)

if __name__ == "__main__":
    main()