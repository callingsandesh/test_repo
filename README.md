## Physical Implemation

###  `utils.py` file to connect to the database.
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
As ,user_name , password , host , ports are the sensitive data , I have placed it into the .env file and imported in this `utils.py` and used it here.

###  Creating the raw  table ,according to the linear keys name of the .json, which is slightly jumbled in documentation provided by yelp.



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




### The below function is used to insert data on bulk to the database tables.
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




### After that, I used the pipeline to push the data into the raw tables.
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

```
if __name__=='__main__':
    database_name="yelp_db"
    batch_size=10000

    file_paths_and_query_path ={
                'user':['../../datasets/yelp_dataset/yelp_academic_dataset_user.json','../sql/insert_into_raw_user.sql'],
                'business':['../../datasets/yelp_dataset/yelp_academic_dataset_business.json','../sql/insert_into_raw_business.sql'],
                'photos':['../../datasets/yelp_dataset/photos.json','../sql/insert_into_raw_photo.sql'],
                'tip':['../../datasets/yelp_dataset/yelp_academic_dataset_tip.json','../sql/insert_into_raw_tip.sql'],
                'review':['../../datasets/yelp_dataset/yelp_academic_dataset_review.json','../sql/insert_into_raw_review.sql'],
                'checkin':['../../datasets/yelp_dataset/yelp_academic_dataset_checkin.json','../sql/insert_into_raw_checkin.sql']
                }
    
    for paths in file_paths_and_query_path.values():
        extract_data(paths[0],paths[1],database_name,batch_size)
```
The above main method is used to pass the file_path and the query_path ,databasename and batch_size to the `extract_date()` method, for the execution.


### After the above all datas are inserted , I create the schemas for the database warehouse.
The following are the list of schemas:
> `schema\create_fact_business.sql`
```
create table fact_business(
		business_id varchar(255) primary key,
		name varchar(255),
		address varchar(255),
		city varchar(255),
		state varchar(255),
		postal_code varchar(255),
		location point,
		stars numeric(2,1),
		review_count int,
		is_open boolean,
		hours_monday varchar(255),
		hours_tuesday varchar(255),
		hours_wednesday varchar(255),
		hours_thursday varchar(255),
		hours_friday varchar(255),
		hours_saturday varchar(255),
		hours_sunday varchar(255),
		wheelchairaccessible boolean,
		parking_garage boolean,
		parking_street boolean,
		parking_validated boolean,
		parking_lot boolean,
		parking_valet boolean,
		businessacceptscreditcards boolean,
		outdoorseating boolean,
		noiselevel varchar(255),
		restaurantsdelivery boolean,
		wifi varchar(255),
		restaurantsattire varchar(255),
		restaurantsgoodforgroups boolean,
		corkage boolean,
		caters boolean,
		restaurantsreservations boolean,
		alcohal text,
		goodforkids boolean,
		restaurantspricerange2 smallint,
		restaurantstakeout boolean,
		
		ambience_touristy boolean,
		ambience_intimate boolean, 
		ambience_romantic boolean,
		ambience_hipster boolean,
		ambience_divey boolean,
		ambience_classy boolean,
		ambience_trendy boolean,
		ambience_upscale boolean,
		ambience_casual boolean,
		
		goodformeal_dessert boolean,
		goodformeal_latenight boolean,
		goodformeal_lunch boolean,
		goodformeal_dinner boolean,
		goodformeal_brunch boolean,
		goodformeal_breakfast boolean,
		
		bikeparking boolean,
		byobcorkage boolean,
		hastv boolean,
		byappointmentonly boolean,
		happyhour boolean,
		restaurantstableservice boolean,
		dogsallowed boolean,
		smoking varchar(255),
		music json,
		byob boolean,
		coatcheck boolean,
		bestnights json,
		goodfordancing boolean
	)
```
> `schema\create_table_fact_user.sql`
```
create table fact_user(
	user_id VARCHAR(22) primary KEY,
	name VARCHAR(255) not NULL,
	review_count INT,
	yelping_since TIMESTAMP,
	useful INT,
	funny INT,
	cool INT,
	fans INT,
	friends_count INT,
	average_stars NUMERIC,
	compliment_hot INT,
	compliment_more INT,
	compliment_profile INT,
	compliment_cute INT,
	compliment_list INT,
	comliment_note INT,
	compliment_plain INT,
	compliment_cool INT,
	compliment_funny INT,
	compliment_writer INT,
	compliment_photos INT
)
```

> `schema\create_table_dim_elite`
```
create table dim_elite(
	user_id VARCHAR(255),
	elite_year CHAR(4),
	
	constraint fk_dim_elite_fact_user_user_id
	foreign key(user_id) references fact_user(user_id),
	
	primary KEY( user_id,elite_year)
)
```

> `schema\create_table_total_user_tip_count.sql`
```
create table total_user_tip_count(
	user_id VARCHAR(255) primary key ,
	total_tip_count smallint,
	constraint fk_total_user_tip_count
	foreign key(user_id) references fact_user(user_id)
)
```

> `schema\create_table_fact_review.sql`
```
create table fact_review(
	review_id VARCHAR(255) primary KEY,
	user_id VARCHAR(255) ,
	business_id VARCHAR(255),
	stars NUMERIC,
	date DATE,
	text text,
	usefull INT,
	funny INT,
	cool INT,
	
	constraint fk_review_fact_user_user_id 
	foreign KEY(user_id) references fact_user(user_id),
	
	constraint fk_review_review_fact_business_business_id
	foreign KEY(business_id) references fact_business(business_id)
)
```

> `schema\create_table_fact_tip.sql`
```
create table fact_tip(
	user_id VARCHAR(255),
	business_id VARCHAR(255),
	text TEXT,
	date TEXT,
	compliment_count VARCHAR(255),
	
	constraint fk_fact_tip_fact_fact_user_user_id
	foreign KEY(user_id) references fact_user(user_id),
	
	constraint fk_fact_tip_fact_business_business_id
	foreign key(business_id) references fact_business(business_id)
	
)
```

> `schema\create_table_fact_checkin.sql`
```
create table fact_checkin(
	business_id VARCHAR(255) primary KEY,
	first_checkin TIMESTAMP,
	last_checkin TIMESTAMP,
	total_checkin INT,
	constraint fk_fact_checkin_fact_business_business_id
	foreign key(business_id) references fact_business(business_id)
)
```

> `schema\create_table_dim_photo.sql`
```
create table dim_photo(
	photo_id VARCHAR(255),
	business_id VARCHAR(255),
	caption text,
	label VARCHAR (255),
	
	constraint fk_dim_photo_fact_business_business_id
	foreign KEY(business_id) references fact_business(business_id)
)
```

> `schema\create_table_dim_photo_count.sql`

```
create table dim_photo_count(
	business_id VARCHAR(255),
	total_photos smallint,
	
	constraint fk_dim_photo_count_fact_business_total_photos
	foreign key(business_id) references fact_business(business_id)
	
)
```
> `schema\create_table_dim_category.sql`
```
create table dim_category(
	id SERIAL primary key,
	name varchar(255)
)
```

> `schema\create_table_link_fact_business_dim_category.sql`
```
create table link_fact_business_dim_category(
	business_id VARCHAR(255),
	category_id SMALLINT
)
```
> `schema\create_table_dim_total_review_words_count.sql`
```
create table dim_total_review_words_count(
	business_id VARCHAR(255) primary KEY,
	total_review_words_count BIGINT,
	total_review_distinct_words_count BIGINT,
	constraint k_dim_total_review_words_count_business_id
	foreign KEY(business_id) references fact_business(business_id)
)
```

> `schema\create_table_total_tip_count.sql`
```create table total_tip_count(
	business_id VARCHAR(255) primary KEY,
	total_tip_count smallint,
	
	constraint fk_total_tip_count_fact_business
	foreign key(business_id) references fact_business(business_id)
)

```
> `schema\create_table_dim_review_count.sql`
```
create table dim_review_count(
	business_id VARCHAR(255) primary key,
	review_count smallint,
	constraint fk_dim_review_count_fact_business_business_id
	foreign key(business_id) references fact_business(business_id)
)
```

### After the above table was created , I used the folloing pipeline to push the datas from the raw table to the above tables of the warehouse.

> `src\extract_into_warehouse_tables.py`
This above file , `extract_into_warehouse_tables.py` has 1 function and 1 main method which is mentioned below:
```
def extract_data(query_path,splitter , database_name,batchsize):
    
    #opening the query file path 
    with open(query_path,'r') as query:
        query=query.read()
    

    split_loc = query.find(splitter)
    select_query = query[split_loc:]
    print(select_query)
    
    insert_query = str(query[:split_loc])+"VALUES %s"
    print(insert_query)
    connection=connect(database_name)
    with connection.cursor(name='cur') as cursor:

        cursor.itersize = 10000
        cursor.execute(select_query)
        i=0
        data=[]
        for row in cursor:
            data.append(row)
            i+=1
            if i==batchsize:

                execute_bulk_insert(insert_query,connect(database_name),data)
                i=0
                data=[]
                continue
        ##inserting remaining data
        if data!=[]:
            execute_bulk_insert(insert_query,connect(database_name),data)
```
The above function takes `query_path`, `splitter` , `database_name` and `batchsize` as an argument to the function.
It first opens the query and splits it from the splitter key-word , so as to form the select statement and insert statement.
After that it iterates over the raw_tables rows one by one and bulk inserts the data after it reaches the batch size.
So, the remaining data left is inserted at the last.



### The description of the above query_path queries which is used to extract data from the raw tables and insert into the warehouse tables are:

> `src\sql\insert_into_fact_user.sql`
```
INSERT INTO fact_user
	SELECT
		user_id,
		name,
		review_count::INT,
		yelping_since::TIMESTAMP,
		useful::INT,
		funny::INT,
		cool::INT,
		fans::INT,
		array_length(regexp_split_to_array(friends, ','), 1) as friends_count,
		average_stars::NUMERIC,
		compliment_hot::INT,
		compliment_more::INT,
		compliment_profile::INT,
		compliment_cute::INT,
		compliment_list::INT,
		comliment_note::INT,
		compliment_plain::INT,
		compliment_cool::INT,
		compliment_funny::INT,
		compliment_writer::INT,
		compliment_photos::INT
	FROM raw_user
```

> `src\sql\insert_into_dim_elite.sql`
```
insert into dim_elite
SELECT 
	   user_id,
       unnest(string_to_array(elite, ',')) as elite_year
from (
--data cleaning 
select user_id,replace(elite,'20,20','2020') as elite
from raw_user ru 
)r
```

> `src\sql\insert_into_total_user_tip_count.sql`
```
INSERT INTO total_user_tip_count
SELECT 
    user_id,
    count(*) as total_tip_count
FROM fact_tip
GROUP BY user_id
ORDER BY total_tip_count DESC
```

> `src\sql\insert_into_fact_business.sql`
```
INSERT into fact_business 
WITH CTE_all_business as (
	select  
	*,
	replace(replace(replace(cast(attributes::json->'WheelchairAccessible' as text),'True','1'),'False','0'),'"','') as WheelchairAccessible,
	replace(replace(replace(replace(replace(cast(attributes::json->'BusinessParking' as text),'"',''),'''','"'),'False','0'),'True','1'),'None','null')::json as BusinessParking,
	replace(replace(replace(cast(attributes::json->'BusinessAcceptsCreditCards' as text),'True','1'),'False','0'),'"','') as BusinessAcceptsCreditCards,
	replace(replace(replace(cast(attributes::json->'OutdoorSeating' as text),'True','1'),'False','0'),'"','') as OutdoorSeating,
	replace(cast(attributes::json->'NoiseLevel' as text),'"','') as NoiseLevel,
	replace(replace(replace(cast(attributes::json->'RestaurantsDelivery' as text),'True','1'),'False','0'),'"','') as RestaurantsDelivery,
	replace(cast(attributes::json->'WiFi' as text),'"','') as wifi,
	replace(cast(attributes::json->'RestaurantsAttire' as text),'"','') as RestaurantsAttire,
	replace(replace(replace(cast(attributes::json->'RestaurantsGoodForGroups' as text),'True','1'),'False','0'),'"','') as RestaurantsGoodForGroups,
	replace(replace(replace(cast(attributes::json->'Corkage' as text),'True','1'),'False','0'),'"','') as Corkage,
	replace(replace(replace(cast(attributes::json->'Caters' as text),'True','1'),'False','0'),'"','') as Caters,
	replace(replace(replace(cast(attributes::json->'RestaurantsReservations' as text),'True','1'),'False','0'),'"','') as RestaurantsReservations,
	replace(cast(attributes::json->'Alcohol' as text),'"','') as Alcohol,
	replace(replace(replace(cast(attributes::json->'GoodForKids' as text),'True','1'),'False','0'),'"','') as GoodForKids,
	replace(replace(cast(attributes::json->'RestaurantsPriceRange2' as text),'None','null'),'"','') as RestaurantsPriceRange2,
	replace(replace(replace(cast(attributes::json->'RestaurantsTakeOut' as text),'True','1'),'False','0'),'"','') as RestaurantsTakeOut,
	replace(replace(replace(replace(replace(cast(attributes::json->'Ambience' as text),'"',''),'''','"'),'False','0'),'True','1'),'None','null')::json as Ambience,
	replace(replace(replace(replace(replace(cast(attributes::json->'GoodForMeal' as text),'"',''),'''','"'),'False','0'),'True','1'),'None','null')::json as GoodForMeal,
	replace(replace(replace(cast(attributes::json->'BikeParking' as text),'True','1'),'False','0'),'"','') as BikeParking,
	replace(replace(cast(attributes::json->'BYOBCorkage' as text),'"',''),'''','') as BYOBCorkage,
	replace(replace(replace(cast(attributes::json->'HasTV' as text),'True','1'),'False','0'),'"','') as HasTV,
	replace(replace(replace(cast(attributes::json->'ByAppointmentOnly' as text),'True','1'),'False','0'),'"','') as ByAppointmentOnly,
	replace(replace(replace(cast(attributes::json->'HappyHour' as text),'True','1'),'False','0'),'"','') as HappyHour,
	replace(replace(replace(cast(attributes::json->'RestaurantsTableService' as text),'True','1'),'False','0'),'"','') as RestaurantsTableService,
	replace(replace(replace(cast(attributes::json->'DogsAllowed' as text),'True','1'),'False','0'),'"','') as DogsAllowed,
	replace(replace(cast(attributes::json->'Smoking' as text),'"',''),'''','') as Smoking,
	replace(replace(replace(replace(replace(cast(attributes::json->'Music' as text),'"',''),'''','"'),'False','0'),'True','1'),'None','null')::json as Music,
	replace(replace(replace(cast(attributes::json->'BYOB' as text),'True','1'),'False','0'),'"','') as BYOB,
	replace(replace(replace(cast(attributes::json->'CoatCheck' as text),'True','1'),'False','0'),'"','') as CoatCheck,
	replace(replace(replace(replace(replace(cast(attributes::json->'BestNights' as text),'"',''),'''','"'),'False','0'),'True','1'),'None','null')::json as BestNights,
	replace(replace(replace(cast(attributes::json->'GoodForDancing' as text),'True','1'),'False','0'),'"','') as GoodForDancing,
	replace(cast(hours::json->'Monday' as text),'"','') as hours_monday,
	replace(cast(hours::json->'Tuesday' as text),'"','') as hours_Tuesday,
	replace(cast(hours::json->'Wednesday' as text),'"','') as hours_Wednesday,
	replace(cast(hours::json->'Thursday' as text),'"','') as hours_Thursday,
	replace(cast(hours::json->'Friday' as text),'"','') as hours_Friday,
	replace(cast(hours::json->'Saturday' as text),'"','') as hours_Saturday,
	replace(cast(hours::json->'Sunday' as text),'"','') as hours_Sunday 
	
	from raw_business rb
	)
	select 
		business_id,
		name,
		address,
		city,
		state,
		postal_code,
		POINT(latitude::numeric,longitute::numeric),
		cast(stars as numeric),
		review_count::int ,
		is_open::boolean ,
		hours_monday,
		hours_tuesday,
		hours_wednesday,
		hours_thursday,
		hours_friday,
		hours_saturday,
		hours_sunday,
		
		case when WheelchairAccessible='None' then null
		else WheelchairAccessible::boolean
		end WheelchairAccessible,
		
		case when cast(BusinessParking->'garage' as text)='null' THEN null
		else cast(cast(BusinessParking->'garage' as text) as boolean)
		end as parking_garage,
		case when cast(BusinessParking->'street' as text)='null' then null
		else cast(cast(BusinessParking->'street' as text) as boolean) 
		end as parking_street,
		case when cast(BusinessParking->'validated' as text)='null' then null
		else cast(cast(BusinessParking->'validated' as text) as boolean) 
		end as parking_validated,
		case when cast(BusinessParking->'lot' as text)='null' then null
		else cast(cast(BusinessParking->'lot' as text)as boolean) 
		end as  parking_lot,
		case when cast(BusinessParking->'valet' as text)='null' then null
		else cast(cast(BusinessParking->'valet' as text)as boolean) 
		end as parking_valet,
		
		case when BusinessAcceptsCreditCards='None' then null
		else BusinessAcceptsCreditCards::boolean
		end BusinessAcceptsCreditCards,
		
		case when OutdoorSeating='None' then null
		else OutdoorSeating::boolean
		end OutdoorSeating,
		
		case when noiselevel='None' then null
		else noiselevel
		end as noiselevel,
		
		case when RestaurantsDelivery='None' then null
		else RestaurantsDelivery::boolean
		end RestaurantsDelivery,
		
		case when WiFi='None' then null
		else WiFi
		end as WiFi,
		

		case when RestaurantsAttire='None' then null
		else RestaurantsAttire
		end as RestaurantsAttire,
		
		case when RestaurantsGoodForGroups='None' then null
		else RestaurantsGoodForGroups::boolean
		end RestaurantsGoodForGroups,
		
		case when Corkage='None' then null
		else Corkage::boolean
		end Corkage,
		
		case when Caters='None' then null
		else Caters::boolean
		end Caters,
		
		case when RestaurantsReservations='None' then null
		else RestaurantsReservations::boolean
		end RestaurantsReservations,
		
		case when Alcohol='None' then null
		else Alcohol
		end as Alcohol,
		
		case when GoodForKids='None' then null
		else GoodForKids::boolean
		end GoodForKids,
		
		case when RestaurantsPriceRange2='null' then null
		else RestaurantsPriceRange2::int
		end RestaurantsPriceRange2,
		
		case when RestaurantsTakeOut='None' then null
		else RestaurantsTakeOut::boolean
		end RestaurantsTakeOut,
		
		case when cast(Ambience->'touristy' as text)='null' then null
	        else cast(cast(Ambience->'touristy' as text) as boolean)
	        end as ambience_touristy,
	        case when cast(Ambience->'intimate' as text)='null' then null
	        else cast(cast(Ambience->'intimate' as text) as boolean)
	        end as ambience_intimate,
	        case when cast(Ambience->'romantic' as text)='null' then null
	        else cast(cast(Ambience->'romantic' as text) as boolean)
	        end as ambience_romantic,
	        case when cast(Ambience->'hipster' as text)='null' then null
	        else cast(cast(Ambience->'hipster' as text) as boolean)
	        end as ambience_hipster,
	        case when cast(Ambience->'divey' as text)='null' then null
	        else cast(cast(Ambience->'divey' as text) as boolean)
	        end as ambience_divey,
	        case when cast(Ambience->'classy' as text)='null' then null
	        else cast(cast(Ambience->'classy' as text) as boolean)
	        end as ambience_classy,
	        case when cast(Ambience->'trendy' as text)='null' then null
	        else cast(cast(Ambience->'trendy' as text) as boolean)
	        end as ambience_trendy,
	        case when cast(Ambience->'upscale' as text)='null' then null
	        else cast(cast(Ambience->'upscale' as text) as boolean)
	        end as ambience_upscale,
	        case when cast(Ambience->'casual' as text)='null' then null
	        else cast(cast(Ambience->'casual' as text) as boolean)
	        end as ambience_casual,
		
		case when cast(GoodForMeal->'dessert' as text)='null' then null
	        else cast(cast(GoodForMeal->'dessert' as text) as boolean)
	        end as GoodForMeal_dessert,
	        case when cast(GoodForMeal->'latenight' as text)='null' then null
	        else cast(cast(GoodForMeal->'latenight' as text) as boolean)
	        end as GoodForMeal_latenight,
	        case when cast(GoodForMeal->'lunch' as text)='null' then null
	        else cast(cast(GoodForMeal->'lunch' as text) as boolean)
	        end as GoodForMeal_lunch,
	        case when cast(GoodForMeal->'dinner' as text)='null' then null
	        else cast(cast(GoodForMeal->'dinner' as text) as boolean)
	        end as GoodForMeal_dinner,
	        case when cast(GoodForMeal->'brunch' as text)='null' then null
	        else cast(cast(GoodForMeal->'brunch' as text) as boolean)
	        end as GoodForMeal_brunch,
	        case when cast(GoodForMeal->'breakfast' as text)='null' then null
	        else cast(cast(GoodForMeal->'breakfast' as text) as boolean)
	        end as GoodForMeal_breakfast,
		
		case when BikeParking='None' then null
		else BikeParking::boolean
		end BikeParking,
		
		case when BYOBCorkage='None' then null
		when BYOBCorkage='no' then '0'::boolean
		when BYOBCorkage='uno' then '0'::boolean
		when BYOBCorkage='uyes_corkage' then '1'::boolean
		when BYOBCorkage='uyes_free' then '1'::boolean
		when BYOBCorkage='yes_corkage' then '1'::boolean
		WHEN BYOBCorkage='yes_free' then '1'::boolean
		end as BYOBCorkage,
		
		case when HasTV='None' then null
		else HasTV::boolean
		end HasTV,
		
		case when ByAppointmentOnly='None' then null
		else ByAppointmentOnly::boolean
		end ByAppointmentOnly,
		
		case when HappyHour='None' then null
		else HappyHour::boolean
		end HappyHour,
		
		case when RestaurantsTableService='None' then null
		else RestaurantsTableService::boolean
		end RestaurantsTableService,
		
		case when DogsAllowed='None' then null
		else DogsAllowed::boolean
		end DogsAllowed,
		
		case when smoking='None' then null
		when smoking='uoutdoor' then 'outdoor'
		when smoking='uno' then 'no'
		when smoking='uyes' then 'yes'
		end as smoking,
		cast(Music as text),
		
		case when BYOB='None' then null
		else BYOB::boolean
		end BYOB,
		
		case when CoatCheck='None' then null
		else CoatCheck::boolean
		end CoatCheck,
		
		cast(BestNights as text),
		
		case when GoodForDancing='None' then null
		else GoodForDancing::boolean
		end GoodForDancing
		
	from CTE_all_business
	
	
```

> `src\sql\insert_into_dim_category.sql`

```
INSERT into dim_category(name) 
WITH cte_categoty as (
SELECT 
	   business_id,
       ltrim( UNNEST(string_to_array(categories, ',')) , ' ') as category
FROM raw_business rb
)
SELECT COUNT(distinct category) FROM cte_categoty
```

> `src\sql\insert_into_link_fact_business_dim_category.sql`
```
INSERT into link_fact_business_dim_category
WITH cte_categoty as (
SELECT 
	business_id,
        unnest(string_to_array(categories, ',')) as category
FROM raw_business rb
)
SELECT business_id , id from cte_categoty
INNER JOIN dim_category d
ON cte_categoty.category = d.name
```

> `src\sql\insert_into_fact_review.sql`
```
INSERT INTO fact_review
SELECT 
	review_id ,
	user_id ,
	business_id ,
	stars ,
	useful ,
	funny ,
	cool,
	text ,
	date 
	 
FROM raw_review rr 
```

> `src\sql\insert_into_fact_tip.sql`
```
INSERT INTO fact_tip
SELECT 
    * 
FROM raw_tip
```

> `src\sql\insert_into_dim_photo.sql`
```
INSERT INTO dim_photo
SELECT 
    * 
FROM raw_photo
```

> `src\sql\insert_into_dim_photo_count.sql`
```
INSERT INTO dim_photo_count
SELECT business_id, count(*) as total_photos
    FROM raw_photo rp 
    GROUP BY business_id 
    ORDER BY total_photos DESC

```

> `src\sql\insert_into_dim_review_count.sql`

```
INSERT INTO dim_review_count
SELECT 
    business_id ,
    count(*) AS  review_count
FROM fact_review fr 
GROUP BY business_id
ORDER BY review_count desc
```

> `src\sql\insert_into_fact_checkin.sql`

```
INSERT INTO fact_checkin
WITH cte_checkin AS (
SELECT
	business_id,
	unnest(string_to_array("date", ',')) as checkin
FROM raw_checkin
)
SELECT business_id, MIN(cast(checkin AS TIMESTAMP)) as first_checkin , MAX(cast(checkin AS TIMESTAMP)) AS last_checkin,COUNT(*) AS total_checkin
FROM cte_checkin
GROUP BY business_id
```

> `src\sql\insert_into_total_tip_count.sql`
```
INSERT INTO total_tip_count 
SELECT 
    business_id,
    count(*) FROM fact_tip
GROUP BY business_id
ORDER BY count DESC
```


> `src\sql\insert_into_dim_total_review_words_count.sql`
```
INSERT INTO dim_total_review_words_count
WITH arranged AS
(
  SELECT 
  		business_id, 
  		UNNEST(STRING_TO_ARRAY(REGEXP_REPLACE(
    	REGEXP_SPLIT_TO_TABLE("text", ','), '[^\w\s]', '', 'g'), ' ')) as "word"
  FROM fact_review
) 
SELECT a.business_id, COUNT(a.word) as total_words,COUNT(distinct a.word)
FROM arranged a
WHERE LENGTH(word) > 0
GROUP BY a.business_id
```

## The main method to execute above queries
> `src\extract_into_warehouse_tables.py`

```
if __name__=='__main__':
    database_name="yelp_db"
    batchsize=10000
    
    #time_to_execute_the_function=input("Enter the time to schedule the query which takes long time in the format ,'%Y-%m-%d %H:%M:%S' ")
    query_path_and_splitter = {
        
        '../sql/insert_into_fact_user.sql':'SELECT',
        '../sql/insert_into_dim_elite.sql':'SELECT',
        '../sql/insert_into_fact_business.sql' : 'WITH',
        '../sql/insert_into_dim_category.sql':'WITH',
        '../sql/insert_into_link_fact_business_dim_category.sql':'WITH',
        '../sql/insert_into_fact_review.sql':'SELECT',
        '../sql/insert_into_fact_tip.sql':'SELECT',
        '../sql/insert_into_dim_photo.sql':'SELECT',
        '../sql/insert_into_dim_photo_count.sql':'SELECT',
        '../sql/insert_into_dim_review_count.sql':'SELECT',
        '../sql/insert_into_fact_checkin.sql':'WITH',
        '../sql/insert_into_total_tip_count.sql':'SELECT',
        '../sql/insert_into_total_user_tip_count.sql':'SELECT',
       

        }
    for path,splitter in query_path_and_splitter.items():
        extract_data(path,splitter,database_name,batchsize)


    #place the query which takes very long to execute here so execute it after certain time expires.
    query_path_and_splitter_long_time = {
         '../sql/insert_into_dim_total_review_words_count.sql':'WITH'
    }

    # enter the time to excute the querry after that certain time
    time_after_to_execute_the_function = "2021-10-07 23:59:00"
    
    while True:
        if datetime.now() > datetime.strptime(time_to_execute_the_function, '%Y-%m-%d %H:%M:%S'):
            print("Time reached")
            for path,splitter in query_path_and_splitter_long_time.items():
                extract_data(path,splitter,database_name,batchsize)
            break

```
The above is the main method, from which the execution of the program starts.It has the database_name , batchsize and
query_path_and_splitter(query which executes faster),query_path_and_splitter_long_time(query which takes longer to execute),time_after_to_execute_the_function   as the variable.
So, we loop through the list of query path and pass the parameter to the above method `extract_data(path,splitter,database_name,batchsize)` to execute the queries.
For the queries , which take longer to execute , we can set a time and execute it after that specific time when the server is not too busy.





