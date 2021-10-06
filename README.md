## Physical Implemation

### Firstly creating a raw  table ,according to the linear keys name of the .json, which is slightly jumbled in documentation provided by yelp.



-> schema\create_table_raw_business.sql

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





After that, used the pipeline to push the data to the raw tables.


