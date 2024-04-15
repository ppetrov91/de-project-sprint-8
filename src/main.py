from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
import pyspark.sql.functions as F
import config
import logging
import sys


logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    logger.info("Creating Spark session")

    try:
        return SparkSession.builder.appName("Test") \
                                   .master("local[*]") \
                                   .config("spark.jars.packages", config.spark_jars_packages) \
                                   .getOrCreate()
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        sys.exit(-1)


def get_user_schema() -> StructType:
    return StructType([
                        StructField("restaurant_id", StringType(), True),
                        StructField("adv_campaign_id", StringType(), True),
                        StructField("adv_campaign_content", StringType(), True),
                        StructField("adv_campaign_owner", StringType(), True),
                        StructField("adv_campaign_owner_contact", StringType(), True),
                        StructField("adv_campaign_datetime_start", TimestampType(), True),
                        StructField("adv_campaign_datetime_end", TimestampType(), True),
                        StructField("datetime_created", TimestampType(), True)
                      ])


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    logger.info("Reading restaurants data from kafka")

    userSchema = get_user_schema()

    try:
        return (spark.readStream
                     .format("kafka")
                     .options(**config.kafka_options)
                     .option("subscribe", config.src_topic)
                     .load()
                     .select(F.from_json(F.col("value").cast("string"), userSchema).alias("value"))
                     .selectExpr("value.*")
               )
    except Exception as e:
        logger.error(f"Error reading restaurants data from Kafka: {str(e)}")
        return None


def filter_stream_data(df: DataFrame) -> DataFrame:
    if df is None:
        return

    logger.info("Filtering restaurants data")

    return (df.dropDuplicates(["restaurant_id", "adv_campaign_id", "adv_campaign_datetime_start"])
              .withWatermark("datetime_created", "10 minutes")
              .withColumn("trigger_datetime_created", F.current_timestamp())
              .filter("trigger_datetime_created >= adv_campaign_datetime_start and trigger_datetime_created <= adv_campaign_datetime_end")
           )


def read_subscribers_data(spark: SparkSession) -> DataFrame:
    logger.info("Reading subscribers data from PostgreSQL")

    try:
        return spark.read.format("jdbc").options(**config.pg_settings["src"]).load()
    except Exception as e:
        logger.error(f"Error reading subscribers data from PostgreSQL: {str(e)}")
        return None


def join_and_transform_data(restaurants_data: DataFrame, subscribers_data: DataFrame) -> DataFrame:
    if restaurants_data is None or subscribers_data is None:
        return

    logger.info("Joining dataframes and transforming data")

    return (restaurants_data.join(subscribers_data, "restaurant_id", how="inner")
                            .withColumn('feedback', F.lit(None).cast(StringType()))
                            .select("restaurant_id", "adv_campaign_id", "adv_campaign_content",
                                    "adv_campaign_owner", "adv_campaign_owner_contact",
                                    "adv_campaign_datetime_start", "adv_campaign_datetime_end",
                                    "datetime_created", "client_id", "trigger_datetime_created", "feedback")
           )


def write_to_postgresql(df: DataFrame) -> None:
    logger.info("Saving data to PostgreSQL")

    try:
        df.write.mode("append").format("jdbc").options(**config.pg_settings["dst"]).save()
    except Exception as e:
        logger.error(f"Error writing data to PostgreSQL: {str(e)}")


def write_to_kafka(df):
    logger.info("Saving data to Kafka")

    try:
        df = df.select(F.to_json(F.struct(F.col("*"))).alias("value"))

        (df.write.format("kafka").mode("append").option("truncate", False)
           .options(**config.kafka_options).option("topic", config.dst_topic)
           .save()
        )
    except Exception as e:
        logger.error(f"Error writing data to Kafka: {str(e)}")


def save_to_postgresql_and_kafka(df, epoch_id) -> None:
    if df is None:
        return

    df.persist()
    write_to_postgresql(df)
    write_to_kafka(df)
    df.unpersist()


def main():
    spark = create_spark_session()
    restaurant_read_stream_df = read_kafka_stream(spark)
    filtered_data = filter_stream_data(restaurant_read_stream_df)
    subscribers_data = read_subscribers_data(spark)
    result_df = join_and_transform_data(filtered_data, subscribers_data)

    (result_df.writeStream.foreachBatch(save_to_postgresql_and_kafka)
              .option("checkpointLocation", "stream_dir")
              .start().awaitTermination()
    )


if __name__ == "__main__":
    main()