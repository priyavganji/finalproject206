import sqlite3
import json
import os
import requests
import csv
import matplotlib.pyplot as plt

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

# CREATE TABLE FOR artworks IN DATABASE
def create_artworks_table(cur, conn):

    #Used for resetting database
    #cur.execute("DROP TABLE IF EXISTS Artworks")
    cur.execute("CREATE TABLE if NOT EXISTS Artworks (id INTEGER PRIMARY KEY, title TEXT, date_start INTEGER TEXT, date_end INTEGER TEXT)")
  
    conn.commit()
    

def add_artworks_from_json(page, cur, conn):

    base_url = f'https://api.artic.edu/api/v1/artworks?page={page}&limit=25'
    r = requests.get(base_url)
   

    j_data = json.loads(r.content)

    for key in j_data.keys():
        
        if key == 'data':
            for dict in j_data['data']:
                id = dict['id']
                title = dict['title']
                date_start = dict['date_start']
                date_end = dict['date_end']
               
                cur.execute("INSERT OR IGNORE INTO Artworks (id,title,date_start, date_end) VALUES (?,?,?,?)", (id,title,date_start,date_end))

    conn.commit()

    pass

def do_calc(cur, conn, file_name):

    calculations = {}
    
    cur.execute("SELECT AVG(date_start) FROM ARTWORKS")
    
    for row in cur:
        
        calculations['AVG start year is: '] = row[0]
       

    cur.execute("SELECT MAX(date_start) FROM ARTWORKS")
    
    for row in cur:
        
        calculations['MAX start year is: '] = row[0]
        

    cur.execute("SELECT MIN(date_start) FROM ARTWORKS")
    
    for row in cur:
        
        calculations['MIN start year is: '] = row[0]
        

    cur.execute("SELECT AVG(date_end) FROM ARTWORKS")
    
    for row in cur:
        
        calculations['AVG end year is: '] = row[0]
        

    cur.execute("SELECT MAX(date_end) FROM ARTWORKS")
    
    for row in cur:
        
        calculations['MAX end year is: '] = row[0]
        

    cur.execute("SELECT MIN(date_end) FROM ARTWORKS")
    
    for row in cur:
        calculations['MIN end year is: '] = row[0]
        
    cur.execute("SELECT AVG(date_end - date_start) FROM ARTWORKS")
    for row in cur:
        calculations['Average # of years an art piece took'] = row[0]

    
    conn.commit()

    '''
    variables I can return (avg) (max)(min) (an artwork that took the most years to complete)
    Need to wrtie out to csv file into a table
    
    '''
    #print(calculations)

    with open(file_name, "w") as write_file:
        json.dump(calculations, write_file, indent=4)

    pass


def visual(cur, conn):

    #pulled data for end dates, only ones that are int type

    cur.execute("SELECT date_end FROM ARTWORKS")
    end_dates = []
    for row in cur:
        
        if (type(row[0])) == int:
            end_dates.append(row[0])
        else:
            continue
   

    conn.commit()


    plt.xlabel("End Years")
    plt.ylabel("Total # of Occurrences")
    plt.title("Artwork End Year Distributions")

    plt.hist([end_dates], bins=[-3000,-2000 -1000, 0, 1000, 2000, 3000], rwidth=0.90, color='blue', label='end_years')

    plt.legend()
    plt.show()

    pass

def visual2(cur, conn):

    # Going more indepth with the data to see the distribution for each year

    cur.execute("SELECT date_end FROM ARTWORKS")
    end_dates2 = []
    for row in cur:
        #print(type(row[0]))
        if (type(row[0])) == int:
            if row[0] >= 1000 and row[0] <= 2000:
                end_dates2.append(row[0])
        else:
            continue
    #print(end_dates2)

    conn.commit()

    plt.xlabel("End Years 1000 - 2000")
    plt.ylabel("Total # of Occurrences")
    plt.title("Artwork End Year Distributions 1000 - 2000")

    plt.hist([end_dates2], bins=[1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000], rwidth=0.80, color='purple', label='end_years')

    plt.legend()
    plt.show()

    pass

def main():
    # SETUP DATABASE AND TABLE
    cur, conn = setUpDatabase('aichicago.db')

    create_artworks_table(cur, conn)

    #adding to the table 25 at a time from each page (6 pages)

    add_artworks_from_json(1, cur, conn)
    add_artworks_from_json(2, cur, conn)
    add_artworks_from_json(3, cur, conn)
    add_artworks_from_json(4, cur, conn)
    add_artworks_from_json(5, cur, conn)
    add_artworks_from_json(6, cur, conn)
    add_artworks_from_json(7, cur, conn)
    add_artworks_from_json(8, cur, conn)
    add_artworks_from_json(9, cur, conn)
    add_artworks_from_json(10, cur, conn)
    
    # Doing calculations
    #do_calc(cur, conn)
    do_calc(cur, conn, "AIChicago_cal.json")

    #doing visualization
    # Likely better to comment out each visualization before running them
    visual(cur,conn)
    visual2(cur, conn)
    
if __name__ == "__main__":
    main()