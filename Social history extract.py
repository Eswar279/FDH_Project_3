#!/usr/bin/env python
# coding: utf-8

# In[12]:


import csv
import psycopg2
import json

conn = psycopg2.connect(host="172.16.34.1", port="5432", user="mimic_demo", password="mimic_demo", database="mimic")
cur = conn.cursor()

# Read in the row IDs from the CSV file
with open("rowids.csv", 'r', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    next(reader)  # skip header row
    row_ids = [int(row[0].strip()) for row in reader]

# Execute the SQL query
cur.execute("""
    SELECT row_id, text
    FROM mimiciii.noteevents
    WHERE category = 'Discharge summary'
    AND row_id IN %s;
""", (tuple(row_ids),))
rows = cur.fetchall()

# Extract the social history section from each text and save to a JSON file
results = {}
for row in rows:
    text = row[1]
    # Find the start index of the social history section
    start_index = text.lower().find('social history')
    if start_index == -1:
        # If the social history section is not found, skip this row
        continue
    # Find the end index of the social history section
    end_index = text.lower().find('family history')
    if end_index == -1:
        end_index = len(text)
    # Extract the social history section
    social_history = text[start_index:end_index]
    # Add the social history section to the results dictionary
    results[str(row[0])] = social_history.strip()

# Save the results to a JSON file
with open('social_history.json', 'w') as f:
    json.dump(results, f, indent=4)

# Close the cursor and connection
cur.close()
conn.close()


# In[14]:


import csv
import psycopg2

# Establish a connection to the MIMIC database
conn = psycopg2.connect(host="172.16.34.1", port="5432", user="mimic_demo", password="mimic_demo", database="mimic")
cur = conn.cursor()

# Read in the row IDs from the CSV file
with open("rowids.csv", 'r', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    row_ids = [int(row[0]) for row in reader]

# Execute the SQL query
cur = conn.cursor()
cur.execute("""
    SELECT row_id, subject_id, hadm_id, chartdate, charttime, text
    FROM mimiciii.noteevents
    WHERE category = 'Discharge summary'
    AND row_id IN %s;
""", (tuple(row_ids),))
rows = cur.fetchall()

# Extract the social history section from each note and save to a CSV file
with open('social_history.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['ROW_ID', 'SOCIAL_HISTORY'])
    for row in rows:
        text = row[5]
        # Find the start index of the social history section
        start_index = text.lower().find('social history')
        if start_index == -1:
            # If the social history section is not found, skip this row
            continue
        # Find the end index of the social history section
        end_index = text.lower().find('family history', start_index)
        if end_index == -1:
            end_index = text.lower().find('personal history', start_index)
        if end_index == -1:
            end_index = len(text)
        # Extract the social history section
        social_history = text[start_index:end_index]
        # Write the row ID and social history section to the CSV file
        writer.writerow([row[0], social_history.strip()])

# Close the cursor and connection
cur.close()
conn.close()

