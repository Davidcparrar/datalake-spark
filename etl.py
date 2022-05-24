import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import (
    year,
    month,
    dayofmonth,
    hour,
    weekofyear,
    dayofweek,
)
from pyspark.sql.types import (
    TimestampType,
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
)

config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Read song data and create songs and artists tables

    Args:
        spark (SparkSession): sparks session
        input_data (str): S3 Input folder
        output_data (str): S3 Output folder

    Returns:
        DataFrame: songs data dataframe
    """

    # get filepath to song data file
    song_schema = StructType(
        [
            StructField("num_songs", IntegerType()),
            StructField("artist_id", StringType()),
            StructField("artist_latitude", DoubleType()),
            StructField("artist_longitude", DoubleType()),
            StructField("artist_location", StringType()),
            StructField("artist_name", StringType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("duration", DoubleType()),
            StructField("year", IntegerType()),
        ]
    )
    song_data = "{}/song_data/*/*/*/*.json".format(
        input_data, schema=song_schema
    )

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        ["song_id", "title", "artist_id", "year", "duration"]
    ).dropDuplicates(subset=["song_id"])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(
        "{}/songs_table.parquet".format(output_data)
    )

    # extract columns to create artists table
    artists_table = df.select(
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ).dropDuplicates(subset=["artist_id"])

    artists_table = (
        artists_table.withColumnRenamed("artist_name", "name")
        .withColumnRenamed("artist_location", "location")
        .withColumnRenamed("artist_latitude", "latitude")
        .withColumnRenamed("artist_longitude", "longitude")
    )

    # write artists table to parquet files
    artists_table.write.parquet("{}/artists_table.parquet".format(output_data))


def process_log_data(spark, input_data, output_data):
    """Process log files to create users, time and songplays tables

    Args:
        spark (SparkSession): sparks session
        input_data (str): S3 Input folder
        output_data (str): S3 Output folder
        song_df (DataFrame): songs data dataframe
    """
    # get filepath to log data file
    log_data = "{}/log_data/*/*/*.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    # extract columns for users table
    users_table = (
        df.orderBy("ts", ascending=False)
        .select(["userId", "firstName", "lastName", "gender", "level"])
        .dropDuplicates(subset=["userId"])
    )

    users_table = (
        users_table.withColumnRenamed("userId", "user_id")
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
    )

    # write users table to parquet files
    users_table.write.parquet("{}/users_table.parquet".format(output_data))
    print("Users_table")
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, DoubleType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("date_time", get_datetime(col("start_time")))

    # extract columns to create time table
    time_table = df.select(["start_time", "date_time"]).distinct()
    time_table = time_table.withColumn("hour", hour(col("date_time")))
    time_table = time_table.withColumn("day", dayofmonth(col("date_time")))
    time_table = time_table.withColumn("week", weekofyear(col("date_time")))
    time_table = time_table.withColumn("month", month(col("date_time")))
    time_table = time_table.withColumn("year", year(col("date_time")))
    time_table = time_table.withColumn("weekday", dayofweek(col("date_time")))
    time_table = time_table.dropDuplicates(subset=["start_time"]).drop(
        "date_time"
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(
        "{}/time_table.parquet".format(output_data)
    )
    print("Time_table")
    # extract columns from joined song and log datasets to create songplays table
    song_schema = StructType(
        [
            StructField("num_songs", IntegerType()),
            StructField("artist_id", StringType()),
            StructField("artist_latitude", DoubleType()),
            StructField("artist_longitude", DoubleType()),
            StructField("artist_location", StringType()),
            StructField("artist_name", StringType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("duration", DoubleType()),
            StructField("year", IntegerType()),
        ]
    )
    song_df = "{}/song_data/*/*/*/*.json".format(
        input_data, schema=song_schema
    )
    song_df = song_df.dropDuplicates(subset=["artist_name", "title"])

    songplays_table = df.join(
        song_df,
        (df.artist == song_df.artist_name) & (df.song == song_df.title),
        "left",
    )
    print("After_join")
    songplays_table = songplays_table.select(
        [
            "start_time",
            "userId",
            "level",
            "song_id",
            "artist_id",
            "sessionId",
            "location",
            "userAgent",
            "date_time",
        ]
    )
    songplays_table = songplays_table.withColumn(
        "month", month(col("date_time"))
    )
    songplays_table = songplays_table.withColumn(
        "year", year(col("date_time"))
    )
    songplays_table = songplays_table.withColumn(
        "songplay_id", monotonically_increasing_id()
    )

    songplays_table = (
        songplays_table.withColumnRenamed("userId", "user_id")
        .withColumnRenamed("sessionId", "session_id")
        .withColumnRenamed("userAgent", "user_agent")
    )
    songplays_table.drop("date_time")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(
        "{}/songplays_table.parquet".format(output_data)
    )


def main():
    """Main function that runs the ELT process"""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://dcp-nanodegree"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()
