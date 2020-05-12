#Project Data Lake on AWS by Spark

## Project summary

In this project, we implement a data lake hosted on S3 for a simulated music stream company. Basically, the ELT pipeline will get datasets saved in AWS S3. The extraction process with the use of Spark can be run on a cluster AWS EMR or in a local machine. The transformed tables are then saved in S3 in the form of parquet files (with partition if necessary).

## Datasets
There are two groups of datasets: song dataset and log dataset. The song dataset is used to extract data for **_artists_** table and **_songs_** table. The log dataset is used to extract data for **_users_** and **_time_** table.

## Database schema

We use the star schema to build the fact and dimension tables for the database.

#### Fact table

**songplays:** songplay\_id, start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user_agent

#### Dimension tables

**users:** user\_id, first\_name, last\_name, gender, level

**songs:** song\_id, title, artist\_id, year, duration

**artists:** artist\_id, name, location, latitude, longitude

**time:** start\_time, hour, day, week, month, year, weekday

## Project file structure

We have a folder named data containing a subset of the mentioned above dataset so that we can test the ETL file in the local environment.

The file ```dl.cfg``` contains the IAM key to AWS.

The ```etl.py``` file is the main file to run the ETL  pipeline. It will extract the data from songs dataset and log dataset to create fact and dimension tables by Spark. These tables are then saved in parquet files on S3 (or in a local machine)

## How to run
#### Local environment
We can run the ETL job on local machine by
```python3 etl.py```. Please specify the output and input path.

#### Run on AWS EMR
1. Create a cluster on AWS EMR with an IAM role that can access S3 bucket to load data.

2. Upload the etl.py to the Master node by scp. Please change the permission of the pem key by ```chmod 600 spark-cluster.pem``` before running scp as follows:
``` scp -i spark-cluster.pem etl.py master_node_Endpoint:~/.```

3. submit the spark job
We log in to the master node by ssh or by using putty and then submit the spark job

```spark-submit etl.py --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-memory 2g --executor-core 2
```

