import configparser
from datetime import datetime
import os

import logging
import boto3
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField as SF, StringType, DoubleType, IntegerType, TimestampType 
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_DEFAULT_REGION']=config['AWS']['AWS_DEFAULT_REGION']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
   
    # define song staging table
    
    song_staging_schema = StructType([
                             SF("artist_id", StringType(), False),
                             SF("artist_latitude", StringType(), True),
                             SF("artist_longitude", StringType(), True),
                             SF("artist_location", StringType(), True),
                             SF("artist_name", StringType(), False),
                             SF("song_id", StringType(), False),
                             SF("title", StringType(), False),
                             SF("duration", DoubleType(), False),
                             SF("year", IntegerType(), False)
                             ])
    
    
    # read song data file
    song_df = spark.read.json(path=song_data, schema=song_staging_schema)
    song_df.printSchema()
    song_df.show(5)
    
    
    # extract columns to create songs table
    songs_table = song_df.select('song_id', 'title', 'artist_id', 'year', 'duration')
   
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
                              output_data + 'song_table.parquet',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id']
                              )

    # extract columns to create artists table with distinct entries.
    artists_table = (song_df.select('artist_id', 
                              col('artist_name').alias('name'), 
                              col('artist_location').alias('location'),
                              col('artist_lattitude').alias('lattitude'), 
                              col('artist_longitude').alias('longitude')
                              )).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(
                                output_data + 'artist_table.parquet',
                                mode='overwrite'
                                )


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # define song staging table
    
    log_staging_schema = StructType([
                            SF("artist", StringType(), True),
                            SF("auth", StringType(), False),
                            SF("first_name", StringType(), True),
                            SF("gender", StringType(), True),
                            SF("item_in_session", IntegerType(), False),
                            SF("last_name", StringType(), True),
                            SF("length", DoubleType(), True),
                            SF("level", StringType(), False),
                            SF("location", StringType(), True),
                            SF("method", StringType(), False),
                            SF("page", StringType(), False),
                            SF("registration", DoubleType(), True),
                            SF("session_id", IntegerType(), False),
                            SF("song", StringType(), True),
                            SF("status", IntegerType(), False),
                            SF("ts", DoubleType(), False),
                            SF("user_agent", StringType(), True),
                            SF("user_id", StringType(), True)
                            ])
    
    # read log data file
    
    log_df = log_df.read.json(path=log_data, schema=log_staging_schema)
    
    # filter by actions for song plays
    log_df = log_df.filter(col('page') == 'NextSong')

    # extract columns for users table    
    user_table = (log_df.select('user_id', 'first_name', 'last_name', 'gender', 'level')).distinct()
    
    # write users table to parquet files
    user_table.write.parquet(
                             output_data + 'user_table.parquet',
                             mode='overwrite'
                            )

    # create datetime and start_time from original ts column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0), TimestampType())
    log_df = log_df.withColumn('start_time', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = (log_df.withColumn('hour', hour(start_time))
                    .withColumn('day', dayofmonth(start_time))
                    .withColumn('week', weekofmonth(start_time))
                    .withColumn('month', month(start_time))
                    .withColumn('year', year(start_time))
                    .withColumn('weekday', dayofweek(start_time))
                    .select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
                    ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
                            output_data + 'user_table.parquet',
                            mode='overwrite',
                            partitionBy=['year', 'month']
                            )

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView('song_df')
    log_df.createOrReplaceTempView('log_df')
    time_table.createOrReplaceTempView('time_table')
    
    songplays_table = spark.sql(
                                """
                                SELECT DISTINCT
                                log_df.start_time, log_df.user_Id, log_df.level, song_df.level, song_df.artist_Id, log_df.session_Id,
                                song_df.artist_location AS location, log_df.user_agent, time_table.year, time_table.month
                                FROM song_df 
                                JOIN log_df
                                ON song_df.artist_name == log_df.artist
                                AND song_df.title == log_df.song
                                AND log_df.length == song_df.length
                                JOIN time_table
                                ON log_df.start_time == time_table.start_time
                                """
                                 ).dropDuplicates()
    
    songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write(
                          output_data + 'songplays_table.parquet',
                          mode='overwrite',
                          partitionBy=['year', 'month']
                         )


def main():
    """
    Combines all other functions to the ETL pipline.
    
    """
    spark = create_spark_session()
    
    # declarating input and output S3 path´s
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://test-sparkify-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
