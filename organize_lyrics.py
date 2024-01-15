import os
import polars as pl

#collect the file paths of all documents in the folder.
def collect_paths(folder_path):
    paths = []
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        paths.append(file_path)
    return paths

#Read each file and extract the lyrics in text form.
def read_txt(path):
    with open(path, 'r') as file:
        file_content = file.read()
        return(file_content)
    
#Collect all the filepaths.
paths = collect_paths('/Users/ischneid/Python_R/dalla-dalla-corpus/lyrics')

#Build lyrics list through read_txt function
lyrics_list = []
for path in paths:
    lyrics_list.append(read_txt(path))

#Split lyrics for futher analysis
def split_lyrics(lyrics):
    stanza_list = lyrics.split('\n\n')
    line_list= []
    for stanza in stanza_list:
        line_list.append(stanza.split('\n'))
    return line_list

full_df_schema = {"line": pl.Int64,
                "stanza": pl.Int64,
                "lyric": str}

full_df = pl.DataFrame(schema = full_df_schema)

def make_table (lyrics):
    df_schema = {"line": pl.Int64,
                "stanza": pl.Int64,
                "lyric": str}
    
    df_main = pl.DataFrame(schema = df_schema)

    lyrics = split_lyrics(lyrics)
    stanza_index = list(range(len(lyrics)))
    stanza_label = [i + 1 for i in stanza_index]

    for i in stanza_index:
        stanza = stanza_label[i]
        stanza_text = lyrics[i]
        for lyric in stanza_text:
            line_label = (len(df_main) + 1)

            data = {"line": line_label,
                    "stanza": stanza,
                    "lyric": lyric}
            
            df = pl.DataFrame(data, schema = df_schema)
            df_main.extend(df)

    return(df_main)


for lyrics in lyrics_list:
    full_df.extend(make_table(lyrics))

full_df.write_csv('/Users/ischneid/Python_R/dalla-dalla-corpus/dalla_dalla_corpus.csv')






