import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/*/"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_output = output_data + "songs.parquet"
    songs_table.write.partitionBy("year","artist_id").parquet(songs_output)

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longtitude")
    
    # write artists table to parquet files
    artists_output = output_data + "artists.parquet"
    artists_table.write.parquet(artists_output)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").distinct()
    
    # write users table to parquet files
    users_output = output_data + "users.parquet"
    users_table.write.parquet(users_output)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('starttime',(get_timestamp(df.ts).cast('timestamp')))
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # create a temp view for log data
    df.createOrReplaceTempView("log_data")
    
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT DISTINCT starttime as start_time, hour(starttime) as hour, dayofmonth(starttime) as day, weekofyear(starttime) as week, 
    month(starttime) as month, year(starttime) as year, weekday(starttime) as weekday
    from log_data
    """)
    
    # write time table to parquet files partitioned by year and month
    time_output = output_data + "time.parquet"
    time_table.write.partitionBy("year","month").parquet(time_output)

    # read in song data to use for songplays table
    song_data = input_data + "song_data/A/A/*/"
    song_df = spark.read.json(song_data)

    # create a temp view for song data
    song_df.createOrReplaceTempView("song_data")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT row_number() over (order by sessionId) as songplay_id, starttime as start_time, 
    userId as user_id, level, song_id, artist_id, sessionId as session_id, location, 
    userAgent as user_agent, year(starttime) as year, month(starttime) as month
    FROM song_data s JOIN log_data l ON l.song = s.title AND l.artist = s.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_output = output_data + "songplays.parquet"
    songplays_table.write.partitionBy("year","month").parquet(songplays_output)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-project/"
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
