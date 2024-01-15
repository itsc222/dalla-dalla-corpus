#Make one table that labels each line with its appropriate stanza.
def make_stanza_table(lyrics):
    lyrics = split_lyrics(lyrics)
    stanza_index = list(range(len(lyrics)))

    stanza_label = [i + 1 for i in stanza_index]
    
    row_stanza = {"stanza": pl.Int64,
                "lyric": str}
    
    df_stanza_main = pl.DataFrame(schema = row_stanza)

    for i in stanza_index:
        stanza = stanza_label[i]
        stanza_lyrics = lyrics[i]
        for line in stanza_lyrics:
            data = {"stanza": stanza,
                    "lyric": line}
            df_stanza = pl.DataFrame(data, schema = row_stanza
                                     )
            df_stanza_main.extend(df_stanza)
    return(df_stanza_main)

#Make table with labels of lines to go along with lyrics   
def make_line_table (lyrics):

    lyrics = split_lyrics(lyrics)
    flattened_lines = [item for list in lyrics for item in list]
    line_index = list(range(len(flattened_lines)))
    line_labels = [i + 1 for i in line_index]

    row_line = {"line": pl.Int64,
                "lyric": str}
    
    df_line_main = pl.DataFrame(schema = row_line)

    for i in line_index:
        line = line_labels[i]
        lyric = flattened_lines[i]
        data = {"line": line,
                "lyric": lyric}
        
        df_line = pl.DataFrame(data, schema = row_line)
        df_line_main.extend(df_line)
    return(df_line_main)

#combine both tables through "join" function.
def combine_tables(lyrics):
    df_line = make_line_table(lyrics)
    df_stanza = make_stanza_table(lyrics)
    full_df = df_line.join(df_stanza, on = "lyric", how = "left")
    full_df = full_df.select("line", "stanza", "lyric").sort("line", "stanza").sort("stanza")
    return(full_df)