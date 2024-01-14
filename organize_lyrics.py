import os
import polars as pl

def collect_paths(folder_path):
    paths = []
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        paths.append(file_path)
    return paths


def read_txt(path):
    with open(path, 'r') as file:
        file_content = file.read()
        return(file_content)
    

paths = collect_paths('/Users/ischneid/Python_R/dalla-dalla-corpus/lyrics')

lyrics_list = []
for path in paths:
    lyrics_list.append(read_txt(path))


def split_lyrics(lyrics):
    stanza_list = lyrics.split('\n\n')
    line_list= []
    for stanza in stanza_list:
        line_list.append(stanza.split('\n'))
    return line_list

