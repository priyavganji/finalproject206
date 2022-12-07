import sqlite3
import json
import os
import requests
import pandas as pd

## creating a table and verifying its creation
conn = sqlite3.connect(r'C:\Users\Skyedulla\Desktop\Artworks_db')
cur = conn.cursor()
cur.execute("CREATE TABLE if not exists Art_Works_final (objectID INTEGER,title TEXT,department TEXT,objectBeginDate INTEGER,objectEndDate INTEGER)")
conn.commit()
cur.execute("select * from Art_Works_final")
print(cur.fetchall())


cur.execute("select * from Art_Works_final")
re= pd.DataFrame(cur.fetchall())
print (re)

## fetching the data from API and storing it in database
i=0
var = True
while var==True:
    base_url = f'https://collectionapi.metmuseum.org/public/collection/v1/objects/'+str(i)
    r = requests.get(base_url)
    json_data = json.loads(r.content)
    if len(json_data)>1:
        df=pd.DataFrame([json_data])
        Cleaned_df=df[['objectID','title','department','objectBeginDate','objectEndDate']]
        Cleaned_df['title']=Cleaned_df['title'].astype(str)
        Cleaned_df['department']=Cleaned_df['department'].astype(str)
        cur.execute("select distinct title from Art_Works_final")
        All_Entries=pd.DataFrame(cur.fetchall())
        ##print(All_Entries)
        j=0
        present=False
        while j<len(All_Entries):
            if (All_Entries[0].iloc[j]==Cleaned_df['title'].iloc[0]):
                present=True
            j+=1
        if (present==False):
            Cleaned_df.to_sql('Art_Works_final',conn,if_exists='append',index=False)
            conn.commit()
    i+=1        
    cur.execute("select distinct title from Art_Works_final")
    temp=pd.DataFrame(cur.fetchall())
    if (len(temp)>100):
        var=False 


## verifying if my data has been added into the database table
cur.execute("select * from Art_Works_final")
re= pd.DataFrame(cur.fetchall())
column_names = [i[0] for i in cur.description]
re.columns=column_names
print (re)
