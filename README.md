## Physical Implemation

### Firstly creating a raw  table ,according to the linear keys name of the .json, which is slightly jumbled in documentation provided by yelp.



> `schema\create_table_raw_business.sql`

```
create table raw_business (
	business_id VARCHAR(255),
	name VARCHAR(255),
	address VARCHAR(255),
	city VARCHAR(255),
	state VARCHAR(255),
	postal_code VARCHAR(255),
	latitude VARCHAR(255),
	longitute VARCHAR(255),
	stars VARCHAR(255),
	review_count VARCHAR(255),
	is_open VARCHAR(255),
	attributes TEXT,
	categories TEXT,
	hours TEXT
)
```
> `schema\create_table_raw_user.sql`
```
create table raw_user(
	user_id VARCHAR(255),
	name VARCHAR(255),
	review_count VARCHAR(255),
	yelping_since VARCHAR(255),
	useful VARCHAR(255),
	funny VARCHAR(255),
	cool VARCHAR(255),
	elite TEXT,
	friends TEXT,
	fans VARCHAR(255),
	average_stars VARCHAR(255),
	compliment_hot VARCHAR(255),
	compliment_more VARCHAR(255),
	compliment_profile VARCHAR(255),
	compliment_cute VARCHAR(255),
	compliment_list TEVARCHAR(255)XT,
	comliment_note VARCHAR(255),
	compliment_plain VARCHAR(255),
	compliment_cool VARCHAR(255),
	compliment_funny VARCHAR(255),
	compliment_writer VARCHAR(255),
	compliment_photos VARCHAR(255)
)
```

> `schema\create_table_raw_review.sql`

```
create table raw_review(
	review_id VARCHAR(255),
	user_id VARCHAR(255),
	business_id VARCHAR(255),
	stars VARCHAR(255),
	useful VARCHAR(255),
	funny VARCHAR(255),
	cool VARCHAR(255),
	text TEXT,
	date VARCHAR(255)
)

```

> `schema\create_table_raw_tip`
```
create table raw_tip(
	user_id VARCHAR(255),
	business_id VARCHAR(255),
	text TEXT,
	date TEXT,
	compliment_count VARCHAR(255)
	
)
```

> `schema\create_table_raw_photo`
```
create table raw_photo(
	photo_id VARCHAR(255),
	business_id VARCHAR(255),
	caption text,
	label VARCHAR (255)
	)
```

After the table was create I made the `utils.py` file to connect to the database.
> `schema\src\utils.py`
```
import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv()
user=os.getenv("user")
password=os.getenv("password")
host=os.getenv("host")
port=os.getenv("port")
def connect(database_name):
    return psycopg2.connect(user,
                                  password,
                                  host,
                                  port,
                                  database=database_name
                                  )
```

The below function is used to insert data on bulk to the database tables.
> `src\helper.py`
```

def execute_bulk_insert(query,connection,data):
    '''This function bulk insert the data into the database table given the list of data'''
    
    try:
        cursor = connection.cursor()
        
        execute_values(
        cursor, query,data)

        connection.commit()
        print(query , "sucessfull")
    except(Exception) as e:
        print(e)
    finally:
        if(connection):
            cursor.close()
            connection.close()
            print("connection closed")
```


```
from psycopg2.extras import execute_values
import json

def execute_select_query(query,connection):
    """This is the method to execute the select sql query given the parameter the query and connection method"""
    try:
        conn=connection
        #print(conn)
        cur=conn.cursor()
        cur.execute(query)
        data=cur.fetchall()

    except(Exception) as e:
        print(e)
    finally:
        if(conn):
            cur.close()
            conn.close()
            return data

```


After that, the used the pipeline to push the data to the raw tables.
> `src\execute_into_raw_table.py`
There are 3 function inside this file.The explanation of each one are:
```
def convert_dist_into_list(data):
    '''This function converts the data of type dict into list of values'''
    return [[json.dumps(ele) if type(ele)==dict else ele for ele in item.values() ] for item in data]
```
The above function converts the data given into dictionary into list of values of the dict.

```
def extract_data(file_path,query_path,database_name,batch_size):
    '''This function extracts raw json data into the raw tables'''
    #print(database_name)
    with open(query_path,'r') as q:
        q=q.read()

    with open(file_path,'r') as f:
        data=[]
        i=0
        for line in f:
            data.append(json.loads(line))
            i+=1

            if i==batch_size:
    
                execute_bulk_insert(q,connect(database_name),convert_dist_into_list(data))
                i=0
                data=[]
                continue
        #execute the remaining data
        if data!=[]:
            execute_bulk_insert(q,connect(database_name),convert_dist_into_list(data))
```

The above function takes the file path, query path and batch size as an argument to the function.
It first open the quey path for execution of the data and  then it opens the file path of the data.After the it reads line by line the data and after it reaches the batch number it bulk inserts that data by calling the function execute_bul_insert().


After the above all datas are inserted , I create the schemas for the database warehouse.
The following are the list of schemas:
> `schema\create_`



