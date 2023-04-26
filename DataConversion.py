import csv
import re
import json
import psycopg2
import pandas as pd


# # extract sentence starts with "SOCIAL HISTORY:"
# def extract_social_history(text):
#
#     original_text = text
#     # text str to dict
#     text = text.split('\n\n')
#     text = dict([line.split(':', 1) for line in text if ':' in line])
#     # strip /n from keys
#     text = {k.strip(): v for k, v in text.items()}
#     if "Social History" in text:
#         social_history = text["Social History"]
#     elif "SOCIAL HISTORY" in text:
#         social_history = text["SOCIAL HISTORY"]
#     elif "PAST SOCIAL HISTORY" in text:
#         social_history = text["PAST SOCIAL HISTORY"]
#     else:
#         social_history = None
#     return social_history
# extract sentence which contains start and end index: sometimes sentence starts with ":"
def extract_sentence(text, start_word_index, end_word_index):
    # find the start index of the sentence can start with ":" or "."
    # Use whichever is closer
    # search before start_word_index
    start_index_1 = text.rfind(':', 0, start_word_index)
    start_index_2 = text.rfind('.', 0, start_word_index)
    # check which is closer to the start index
    start_index = max(start_index_1, start_index_2)

    # find the end index of the sentence can end with "." or "\n"
    # Use whichever is closer
    end_index_1 = text.find('.', end_word_index)
    end_index_2 = text.find('\n', end_word_index)
    # check which is closer to the end index
    end_index = min(end_index_1, end_index_2)

    # extract the sentence
    sentence = text[start_index:end_index]

    if start_index == -1 or end_index == -1:
        raise ValueError("start_index: {}, end_index: {}".format(start_index, end_index))

    return sentence, (start_index, end_index)


if __name__ == '__main__':
    train_df = pd.read_csv('train.csv')
    # train_df = train_df[0:10]
    row_ids = train_df['row_id'].tolist()


    # Establish a connection to the MIMIC database
    conn = psycopg2.connect(host="172.16.34.1", port="5432", user="mimic_demo", password="mimic_demo", database="mimic")
    cur = conn.cursor()

    # Execute the SQL query
    cur.execute("""
        SELECT row_id, text
        FROM mimiciii.noteevents
        WHERE category = 'Discharge summary'
        AND row_id IN %s;
    """, (tuple(row_ids),))
    rows = cur.fetchall()
    # to dict row_id: text
    rows = dict(rows)

    # train_df grouped by row_id
    train_df = train_df.groupby('row_id')

    train_df_new = pd.DataFrame(columns=['row_id', 'sentence', 'annotations'])
    for row_id, group in train_df:
        text = rows[row_id]

        min_start = group['start'].min()
        max_end = group['end'].max()

        # extract sentence which contains min_start and max_end
        sentence, (start_index, end_index) = extract_sentence(text, min_start, max_end)

        # update start and end relative to the sentence
        group['start_new'] = group['start'] - start_index
        group['end_new'] = group['end'] - start_index
        anns = {"entities": []}
        for sbdh, start_new, end_new, start,end in zip(group['sbdh'], group['start_new'], group['end_new'], group['start'], group['end']):

            word = sentence[start_new:end_new]
            word_original = text[start:end]

            assert word == word_original, "word: {}, word_original: {}".format(word, word_original)

            anns["entities"].append((start_new, end_new, sbdh))

        dp = {"sentence": sentence, "annotations": anns, "row_id": row_id}
        train_df_new = pd.concat([train_df_new,  pd.DataFrame(dp)], ignore_index=True)


    # save to json file
    train_df_new.to_json('data_train.json', orient='records', lines=True)

    # save to csv file
    train_df_new.to_csv('data_train.csv', index=False)
















