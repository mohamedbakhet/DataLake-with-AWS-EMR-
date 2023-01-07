import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: This function create and configure spark session.

    Arguments:
        No Arguments 
    Return:
        spark  {object}: SparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function responsable for ETL for song data load data from s3 and extract columns wanted and then save it on parquet format 

    Arguments:
        spark {object}: SparkSession object
        input_data {object}: Source S3 endpoint
        output_data {object}: Target S3 endpoint
    Returns:
        None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/*/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # extract columns to create songs table
    songs_table = df.select(col("song_id"), col("title"), col("artist_id"), col("year"),col("duration")).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/').mode("overwrite")


    # extract columns to create artists table
    artists_table = df.select(col("artist_id"), col("artist_name"), col("artist_location"), col("artist_latitude"), col("artist_longitude")).distinct()

    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/').mode("overwrite")


def process_log_data(spark, input_data, output_data):
    """ Process log data and create time and songplays table
    
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """
    # get filepath to log data file
    log_data =os.path.join(input_data, "log-data/")

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong') 

    # extract columns for users table    
    user_table = df.select(col("firstName"), col("lastName"), col("gender"), col("level"), col("userId")).distinct()

    # write users table to parquet files
    user_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(date_convert, TimestampType())
    df = df.withColumn("start_ts", get_datetime('ts')) 
    
    # extract columns to create time table
    time_df = df.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_ts"))\
        .withColumn("day", day(col("start_ts")) \
        .withColumn("week", week(col("start_time"))\
        .withColumn("month", month(col("start_time")) \
        .withColumn("year", year(col("start_time"))\
        .withColumn("weekday", date_format(col("start_time"), 'E'))
                           
    # write time table to parquet files partitioned by year and month
    time_df.write.partitionBy("year", "month").parquet(output_data + 'time/')
                    
    # read in song data to use for songplays table
    song = spark.read.parquet(output_data+'songs/*/*/*')
    df_artists = spark.read.parquet(output_data + 'artists/*')
                
    # extract columns from joined song and log datasets to create songplays table 
                
    
    logs = df.join(songs, (df.song == songs.title))
                    
    all_logs = logs.join(df_artists, (logs.artist == df_artists.name))
    
    songplays = all_logs.join(
        time_df,
        all_logs.ts == time_df.ts, 'left'
    ).drop(all_logs.year)
                    
    songplays.createOrReplaceTempView("songplays")
                    
    songplays_table = spark.sql("SELECT start_time,userId,level,song_id,artist_id,sessionId,location,userAgent,year,month FROM songplays")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquetoutput_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
