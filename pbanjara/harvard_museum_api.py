import requests
import json
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from config import API_KEY

# CREATE DATABASE
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

# CREATE DATABASE TABLE FOR OBJECTS
def create_object_table(cur):
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS object 
    (id INTEGER PRIMARY KEY, 
    title TEXT, 
    datebegin INTEGER, 
    dateend INTEGER, 
    culture TEXT,
    personid INTEGER)
    """)

# CREATE DATABASE TABLE FOR PERSONS
def create_people_table(cur):
   
    cur.execute("""
    CREATE TABLE IF NOT EXISTS person
    (id INTEGER PRIMARY KEY, 
    displayname TEXT)
    """)

# GET OBJECT DATA FROM API
def get_object_records(cur):

    keys = ['id', 'title', 'datebegin', 'dateend', 'culture', 'personid']

    url = f"https://api.harvardartmuseums.org/object?apikey={API_KEY}&classification=Paintings&q=(peoplecount:1 AND datebegin:[1000 TO 2000])&size=100"

    ids = []
    records = []
    while True:
        print(url)
        res = requests.get(url)
        res = json.loads(res.content)

        object_records = res['records']

        for record in object_records:
            if record['id'] not in ids:
                record['personid'] = record['people'][0]['personid']
                record = {key:record[key] for key in keys}
                records.append(record)
                ids.append(record['id'])

        if 'next' in res['info']:
            url = res['info']['next']
        else:
            break

    return records

# GET PEOPLE DATA FROM API
def get_person_records(cur):

    keys = ['id', 'displayname'] 

    personids = cur.execute("""SELECT DISTINCT personid FROM object""")
    personids = list(personids)
    print(personids)
    records=[]
    page = 1
    for start in range(0, len(personids), 100):  
        q = f"({' OR '.join([f'id:{personid[0]}' for personid in personids[start:start+100]])})"

        url = f"https://api.harvardartmuseums.org/person?apikey={API_KEY}&page={page}&size=100&q={q}"
        while True:
            print(url)
            res = requests.get(url)
            res = json.loads(res.content)

            person_records = res['records']

            for record in person_records:
                record = {key:record[key] for key in keys}
                records.append(record)

            if 'next' in res['info']:
                url = res['info']['next']
            else:
                break

    return records

# ADD OBJECT DATA TO DATABASE TABLE
def add_object_records(records, cur):
    
    insert_info = "INSERT INTO object ('id', 'title', 'datebegin', 'dateend', 'culture', 'personid') VALUES (?,?,?,?,?,?)"

    tup_values = [tuple(record.values()) for record in records]

    cur.executemany(insert_info, tup_values)


# ADD PERSON DATA TO DATABASE TABLE
def add_person_records(records, cur):

    insert_info = "INSERT INTO person ('id', 'displayname') VALUES (?,?)"

    for record in records:
        data_tup = tuple(record.values())
        print(data_tup)
        cur.execute(insert_info, data_tup)

#JOIN TABLES ON PERSON ID
def join_tables(cur, conn):
    cur.execute("""
    SELECT object.personid, 
    person.displayname, 
    object.id, 
    object.title, 
    object.datebegin, 
    object.dateend,   
    object.culture
    FROM object
    INNER JOIN person
    ON object.personid = person.id
    """)
    conn.commit()

# CALCULATE MINIMUM, MAXIMUM AND AVERAGE TIME TAKEN FOR THE ARTWORS AND CALCULATE MINIMUM & MAXIMUM  NUMBER OF PAINTINGS BY ARTIST NAME 
def calculate_min_max_avg(cur, conn):
    cur = conn.cursor()
    r = cur.execute("""
    SELECT person.displayname, COUNT(object.id) AS count
    FROM 
    object
    INNER JOIN
    person
    ON object.personid = person.id
    GROUP BY
    personid
    ORDER BY
    count DESC
    """)
    conn.commit()

    rows = r.fetchall()
    max_name = rows[0][0]
    max_num = rows[0][1]
    min_name = rows[-1][0]
    min_num = rows[-1][-1]
    

    res = cur.execute('''SELECT  
    MAX(dateend),
    MIN(dateend),
    ROUND(AVG(dateend), 2),
    MAX(datebegin),
    MIN(datebegin),
    ROUND(AVG(datebegin), 2),
    ROUND(AVG((dateend - datebegin)), 2)
    FROM object WHERE
    datebegin >= 1000 AND datebegin <= 2000 
    AND dateend >= 1000 AND dateend <= 2000''')

    rows = res.fetchall()
    for row in rows:
        max_dateend = row[0]
        min_dateend = row[1]
        avg_dateend = row[2]

        max_datebegin = row[3]
        min_datebegin = row[4]
        avg_datebegin = row[5]

        average_time = row[6]

    f = open("harvard_api.txt", "w")

    f.write(f"Maximum number of Paintings by {max_name} which is {max_num}\nMinimum number of paintings by {min_name} which is {min_num}\nMaximum End Date: Year {max_dateend}\nMinimum End Date: Year {min_dateend}\nAverage End Date: {avg_dateend} years.\nMaximum Begin Date: Year {max_datebegin}\nMinimum Begin Date: Year {min_datebegin}\nAverage Begin Date: {avg_datebegin} years.\nAverage Paintings Completion Time: {average_time} years")

    f.close()

# CREATE A VISUALIZATION FOR THE ARTWORKS BY THE ENDDATE DISTRIBUTION 
def visualization_enddate(cur, conn):

    df = pd.read_sql_query('SELECT dateend from object', conn)
    plt.xlabel("End Date")
    plt.ylabel("Count")
    plt.title("End Year Distributions for Paintings")
    sns.histplot(data=df, x='dateend', bins=[_ for _ in range(1000, 2100, 100)])
    plt.show()
    
# CREATE A VISUALIZATION FOR ARTWORKS BY CULTURAL DISTRIBUTION
def visualization_culture(cur, conn):
    
    df = pd.read_sql_query('SELECT * from object', conn)
    df = df.loc[df['datebegin'] > 1000]
    df = df.loc[df['culture'].notna()]
    df = df['culture'].value_counts().to_frame('count')
    df.index.name = 'culture'
    df = df.reset_index()
    df = df.loc[df['count'] > 10]
    plt.figure(figsize=(10,5))
    axes = sns.barplot(data=df, x="culture", y="count").set(title='Distribution of Paintings by Culture')
    plt.xticks(rotation=90)
    plt.show()


def main():

    # SETUP DATABASE AND TABLE
    cur, conn = setUpDatabase('harvard_api.db')
    create_object_table(cur)
    create_people_table(cur)
    conn.commit()

    records = get_object_records(cur)

    #STORE 25 OBJECT RECORDS AT A TIME INTO THE DATABASE
    for record_start in range(0, len(records), 25):
        add_object_records(records[record_start:record_start+25], cur)

    conn.commit()

    records = get_person_records(cur)

    #STORE 25 PERSON RECORDS AT A TIME INTO THE DATABASE
    for record_start in range(0, len(records), 25):
        add_person_records(records[record_start:record_start+25], cur)

    conn.commit()

    join_tables(cur, conn)
    calculate_min_max_avg(cur, conn)
    visualization_enddate(cur, conn)
    visualization_culture(cur, conn)
    conn.commit()
    conn.close()
    
if __name__ == '__main__':
    main()