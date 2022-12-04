import requests
import json
import os
import sqlite3

from config import API_KEY

# CREATE DATABASE
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

# CREATE DATABASE TABLE FOR OBJECTS
def create_object_table(cur):
    cur.execute("DROP TABLE IF EXISTS object")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS object 
    (id INTEGER PRIMARY KEY, 
    title TEXT, 
    datebegin INTEGER, 
    dateend INTEGER, 
    rank INTEGER, 
    division TEXT, 
    century TEXT, 
    department TEXT,
    culture TEXT,
    personid INTEGER)
    """)

# CREATE DATABASE TABLE FOR PERSONS
def create_people_table(cur):
    cur.execute("DROP TABLE IF EXISTS person")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS person
    (id INTEGER PRIMARY KEY, 
    displayname TEXT)
    """)

# GET OBJECT DATA FROM API
def get_object_records(cur):
    TOTAL_OBJECT_RECORD = 500

    keys = ['id', 'title', 'datebegin', 'dateend', 'rank', 'division', 'century', 'department', 'culture', 'personid']

    url = f"https://api.harvardartmuseums.org/object?apikey={API_KEY}&century=21st century&q=peoplecount:1&size=25"

    ids = []
    records = []
    while True:
        res = requests.get(url)
        res = json.loads(res.content)

        object_records = res['records']

        for record in object_records:
            if record['id'] not in ids:
                record['personid'] = record['people'][0]['personid']
                record = {key:record[key] for key in keys}
                records.append(record)
                ids.append(record['id'])

        if len(records) > TOTAL_OBJECT_RECORD:
            break
        elif 'next' in res['info']:
            url = res['info']['next']
        else:
            break

    return records

# GET PEOPLE DATA FROM API
def get_person_records(cur):

    keys = ['id', 'displayname'] 

    personids = cur.execute("""SELECT DISTINCT personid FROM object""")
    q = f"({' OR '.join([f'id:{personid[0]}' for personid in personids])})"

    records=[]
    page = 1
    url = f"https://api.harvardartmuseums.org/person?apikey={API_KEY}&page={page}&size=25&q={q}"
    while True:
        res = requests.get(url)
        print(res)
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

#ADD OBJECT DATA TO DATABASE TABLE
def add_object_records(records, cur):
    
    insert_info = "INSERT INTO object ('id', 'title', 'datebegin', 'dateend', 'rank', 'division', 'century', 'department', 'culture', 'personid') VALUES (?,?,?,?,?,?,?,?,?,?)"

    for record in records:
        data_tup = tuple(record.values())
        print(data_tup)
        cur.execute(insert_info, data_tup)

#ADD PERSON DATA TO DATABASE TABLE
def add_person_records(records, cur):

    insert_info = "INSERT INTO person ('id', 'displayname') VALUES (?,?);"

    for record in records:
        data_tup = tuple(record.values())
        cur.execute(insert_info, data_tup)

def join_tables(cur, conn):
    cur.execute("""
    SELECT object.personid, 
    person.displayname, 
    object.id, object.title, 
    object.datebegin, 
    object.dateend, 
    object.rank, 
    object.division, 
    object.department, 
    object.century, 
    object.culture
    FROM object
    INNER JOIN person
    ON object.personid = person.id
    """)
    conn.commit()

def main():
    # SETUP DATABASE AND TABLE
    cur, conn = setUpDatabase('harvard_api.db')
    create_object_table(cur)
    create_people_table(cur)
    conn.commit()

    records = get_object_records(cur)
    add_object_records(records, cur)

    records = get_person_records(cur)
    add_person_records(records, cur)

    join_tables(cur, conn)
    conn.commit()
    conn.close()
    
main()