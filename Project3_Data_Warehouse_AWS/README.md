
# PROJECT Data Warehouse on AWS

## Project summary


In this project, we create a data warehouse on AWS for a simulated music company. Datasets in the JSON format are store on AWS S3 and imported to AWS Redshift. These datasets are staged in Redshift by the COPY before being extracted for fact table and dimension tabes.

An ETL pipeline is created to automate the process

## Data

There are two types of datasets: song dataset and log dataset. 
Example of Song data:  
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

Example of Log data:
```
{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}
```

## The Star schema
The Star schema is used to create Fact table and Dimension tables for the DWH
### Songplays
``` 
songplay_id INTEGER IDENTITY(0,1),  
start_time TIMESTAMP,
user_id INTEGER NOT NULL,
level VARCHAR,
song_id VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR)
DISTSTYLE KEY
DISTKEY(artist_id)
SORTKEY(start_time)
```

### Users
```
user_id INTEGER PRIMARY KEY,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
gender CHAR,
level VARCHAR)
SORTKEY(user_id)
```

### Songs
```
song_id VARCHAR NOT NULL,
title VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
year INTEGER,
duration NUMERIC)
SORTKEY(song_id)
```
### Artists
```
artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location VARCHAR,
latitude NUMERIC,
longitude NUMERIC)
SORTKEY (artist_id);
```
### Time
```
start_time TIMESTAMP PRIMARY KEY,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY(start_time);
```

## Project file structure
The project files are organized as follows:  
**sql_queries.py**: this file contains necessary SQL queries to be called in the main file   
**create_tables.py**: this file defines functions to initialize a database, create tables, and drop tables   
**create_cluster.ipynb**: this notebooks is a place to create cluster and delete cluster after finishing the work
**test.py**: this file is used to run SQL commands to check created tables
**etl.py**: this file defines functions necessary for ETL, and the main function   


## How to run

1. We need to create a cluster. Please update the dwh.cfg  
2. Run `python3 create_tables.py`
3. Run `python3 etl.py`
4. Delete cluster when finish