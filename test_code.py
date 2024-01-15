import os
import polars as pl

def collect_paths(folder_path):
    return [os.path.join(folder_path, file) for file in os.listdir(folder_path)]

def read_txt(path):
    with open(path, 'r') as file:
        return file.read()

def make_table(lyrics, current_line):
    df_schema = {"line": pl.Int64, "stanza": pl.Int64, "lyric": str}
    df_main = pl.DataFrame(schema=df_schema)

    lyrics_split = [stanza.split('\n') for stanza in lyrics.split('\n\n')]

    for stanza_label, stanza_text in enumerate(lyrics_split, start=1):
        for lyric in stanza_text:
            current_line += 1
            data = {"line": current_line, "stanza": stanza_label, "lyric": lyric}
            df = pl.DataFrame(data, schema=df_schema)
            df_main = df_main.extend(df)

    return df_main, current_line

folder_path = '/Users/ischneid/Python_R/dalla-dalla-corpus/lyrics'

# Collect file paths and build lyrics list
lyrics_list = [read_txt(path) for path in collect_paths(folder_path)]

full_df_schema = {"line": pl.Int64, "stanza": pl.Int64, "lyric": str}
full_df = pl.DataFrame(schema=full_df_schema)
current_line = 0

for lyrics in lyrics_list:
    df_main, current_line = make_table(lyrics, current_line)
    full_df = full_df.extend(df_main)

full_df.write_csv('/Users/ischneid/Python_R/dalla-dalla-corpus/dalla_dalla_corpus.csv')
