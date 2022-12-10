import sqlite3
import json
import os
import requests
import pandas as pd


# setting up a database table
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath('./'))
    conn = sqlite3.connect(path+'/'+db_name)
    return conn

conn = setUpDatabase('Artworks_db')

conn = sqlite3.connect(r'C:\Users\\Desktop\Artworks_db')
cur = conn.cursor()
cur.execute("CREATE TABLE if not exists Art_Works_final (objectID INTEGER,title TEXT,department TEXT,objectBeginDate INTEGER,objectEndDate INTEGER)")
conn.commit()
cur.execute("select * from Art_Works_final")
print(cur.fetchall())

# check for data
cur.execute("select * from Art_Works_final")
re= pd.DataFrame(cur.fetchall())
print(re)


# fetching the data from API and storing it in database
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


#doing calculations for (1) min,max & avg start and end years,(2) longest painting duration,(3) average painting duration
cur.execute("select * from Art_Works_final")
re= pd.DataFrame(cur.fetchall())
column_names = [i[0] for i in cur.description]
re.columns=column_names
re

max_Begin_Date=re.max(axis=0)['objectBeginDate'] 
min_Begin_Date=re.min(axis=0)['objectBeginDate']
Avg_Begin_Date=re.mean(axis=0)['objectBeginDate']

max_End_Date=re.max(axis=0)['objectEndDate'] 
min_End_Date=re.min(axis=0)['objectEndDate']
Avg_End_Date=re.mean(axis=0)['objectEndDate']

re['Difference']=re['objectEndDate']-re['objectBeginDate'] 
longest_Object_Time=re.max(axis=0)['Difference']
Avg_Object_Time=re.mean(axis=0)['Difference']


#putting data into txt file
f = open(r"C:\Users\Desktop\MyNewOutput.txt", "w")


f.write("Max Begin Date is: "+str(max_Begin_Date)+"\n")
f.write("Min Begin Date is: "+str(min_Begin_Date)+"\n")
f.write("Average Begin Date is: "+str(Avg_Begin_Date)+"\n")
f.write("Max End Date is: "+str(max_End_Date)+"\n")
f.write("Min End Date is: "+str(min_End_Date)+"\n")
f.write("Average End Date is: "+str(Avg_End_Date)+"\n")
f.write("Longest Object Time is: "+str(longest_Object_Time)+"\n")
f.write("Average Object Time is: "+str(Avg_Object_Time)+"\n")
f.close()

#putting data into json file
import json

f = open(r"C:\Users\Desktop\MyNewOutput.txt", "r")
content = f.read()
splitcontent = content.splitlines()

d = []
for v in splitcontent:
    l = v.split(' | ')
    d.append(dict(s.split(':',1) for s in l))


with open("Output.json", 'w') as file:
    file.write((json.dumps(d, indent=4, sort_keys= False)))


#creating histogram
import matplotlib.pyplot as plt
x = re['objectEndDate']
plt.hist(x, bins=30)
plt.ylabel('Total Number of Occurances')
plt.xlabel('Years Range (1600-1900)')
plt.title('Distributions Over End Years Histogram!')  
plt.show()

