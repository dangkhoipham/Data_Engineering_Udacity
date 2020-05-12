import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This functions will create a spark session 
    Return:
        spark:SparkSession()
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function will process the song data. It load the data from input_data and create tables songs_table and artists_table
    Paramenters:
        spark: a SparkSession
        input_data (string): the path to the folder of input data
        output_data (string): the path to the output data
    Return:
        None
    """
    # get filepath to song data file
    song_data = input_data + '/song_data/*/*/*/*'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id","title", "artist_id","year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+"songs/", partitionBy = ["year","artist_id"], mode = "overwrite")

    # extract columns to create artists table
    artists_table = df.select(["artist_id","artist_name","artist_location","artist_longitude","artist_latitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artists/",mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function will process the log files to generate tables: users, times, and songplays
    Parameters:
        spark: an instance of SparkSession
        input_data: the path to the folder of log data
        output_data: the path to write parquets
    """
    # get filepath to log data file
    log_data = input_data + '/log_data/*'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page=='NextSong'")

    # extract columns for users table    
    users_table = df.select(["userId","firstName", "lastName","gender","level"]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users/', mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x/1000)))
    df = df.withColumn("time_stamp", get_timestamp("ts"))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x:datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn("start_time",get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select("start_time", hour("start_time").alias("hour"), \
                dayofmonth("start_time").alias("day"), \
                weekofyear("start_time").alias("week"), \
                month("start_time").alias("month"), \
                year("start_time").alias("year"), \
                dayofweek("start_time").alias("weekday")).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'time/', partitionBy=["year","month"], mode = "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"/songs/*/*/*") 
    
    # read in user data to use for songplays table
    artist_df = spark.read.parquet(output_data+"/artists/*") 

    # extract columns from joined song and log datasets to create songplays table 
    songplay_table = df.join(song_df,df.song == song_df.title)\
                       .join(artist_df, df.artist == artist_df.artist_name)\
                       .select(monotonically_increasing_id().alias("songplay_id"), df.start_time, df.userId.alias("user_id"),\
                               df.level, song_df.song_id, artist_df.artist_id, df.sessionId.alias("session_id"),df.location,\
                                df.userAgent.alias("user_agent"), month(df.start_time).alias("month"), \
                               year(df.start_time).alias("year"))

    # write songplays table to parquet files partitioned by year and month
    songplay_table.write.parquet(output_data+'songplays/', partitionBy=["year","month"], mode = "overwrite")


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "data/"
    output_data = "output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
