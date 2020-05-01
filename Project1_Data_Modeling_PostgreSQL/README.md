# Data Modeling with PostgreSQL in Python

## Summary

In this project, we create a data model by the Star schema for a simulated music company. Python is used to do the ETL job.

## Data

Two kinds of dataset are given in this project in the form of json format. The first one called **Song Dataset** is used to create tables for songs and and artists. The second one called **Log Dataset** provides other information of user.

## Schema

The schema used in this project is Star schema with the fact table is the songplays and 4 dimension tables, namely: users, songs, artists, and time. 

### Songplays
``` 
songplay_id varchar PRIMARY KEY,  
start_time timestamp,  
user_id varchar,  
artist_id varchar,   
session_id int,   
location varchar,   
user_agent varchar  
```

### Users
```
user_id int PRIMARY KEY,   
first_name varchar,    
last_name varchar,   
gender varchar,   
level varchar)
```

### Songs
```
song_id varchar PRIMARY KEY,    
title varchar,    
artist_id varchar,   
year int,    
duration numeric
```
### Artists
```
artist_id varchar PRIMARY KEY,   
name varchar,    
location varchar,    
latitude varchar,    
longitude varchar
```
### Time
```
start_time timestamp PRIMARY KEY,   
hour int,   
day int,   
week int,   
month int,   
year int,    
weekday int
```

## Project file structure
The project files are organized as follows:  
**Data folder**: contains all data set with two subfolders: data_song_file and data_log_file   
**sql_queries.py**: this file contains necessary SQL queries to be called in the main file   
**create_tables.py**: this file defines functions to initialize a database, create tables, and drop tables   
**etl.ipynb**: this notebooks is a place to test ETL process with a single file
**test.py**: this file is used to test the new created tables
**etl.py**: this file defines functions necessary for ETL, and the main function   


## How to run

1. Initialize tables by `create_tables.py`

2. Stream data to the database by `etl.py`



