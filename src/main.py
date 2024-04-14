from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType
import pyspark.sql.functions as F
import config


def spark_init() -> SparkSession:
    return SparkSession.builder.appName("Test") \
                               .master("local[*]") \
                               .config("spark.jars.packages", config.spark_jars_packages) \
                               .getOrCreate()

def load_restaurants_stream(spark: SparkSession) -> DataFrame:
    userSchema = StructType([
                           StructField("restaurant_id", StringType(), True),
                           StructField("adv_campaign_id", StringType(), True),
                           StructField("adv_campaign_content", StringType(), True),
                           StructField("adv_campaign_owner", StringType(), True),
                           StructField("adv_campaign_owner_contact", StringType(), True),
                           StructField("adv_campaign_datetime_start", TimestampType(), True),
                           StructField("adv_campaign_datetime_end", TimestampType(), True),
                           StructField("datetime_created", TimestampType(), True)
                        ])

    return (spark.readStream
                 .format("kafka")
                 .options(**config.kafka_options)
                 .option("subscribe", config.src_topic)
                 .load()
                 .select(F.from_json(F.col("value").cast("string"), userSchema).alias("value"))
                 .selectExpr("value.*")
                 .dropDuplicates(["restaurant_id", "adv_campaign_id", "adv_campaign_datetime_start"])
                 .withWatermark("datetime_created", "10 minutes")
                 .withColumn("trigger_datetime_created", F.current_timestamp())
                 .filter("trigger_datetime_created >= adv_campaign_datetime_start and trigger_datetime_created <= adv_campaign_datetime_end")
                 .withColumn('feedback', F.lit(None).cast(StringType()))
           )

def get_output_df(spark: SparkSession, restaurants_df: DataFrame) -> DataFrame:
    subscribers_restaurants_df = (spark.read.format("jdbc")
                                       .options(**config.pg_settings["src"]).load())

    return (restaurants_df.join(subscribers_restaurants_df, "restaurant_id", how="inner")
                          .select("restaurant_id", "adv_campaign_id", "adv_campaign_content",
                                  "adv_campaign_owner", "adv_campaign_owner_contact",
                                  "adv_campaign_datetime_start", "adv_campaign_datetime_end",
                                  "datetime_created", "client_id", "trigger_datetime_created", "feedback")
           )

def save_data(df, epoch_id) -> None:
    df.persist()

    df.write.mode("append").format("jdbc").options(**config.pg_settings["dst"]).save()

    kafka_df = df.select(F.to_json(F.struct(F.col("*"))).alias("value"))

    (kafka_df.write.format("kafka").mode("append").option("truncate", False)
             .options(**config.kafka_options).option("topic", config.dst_topic)
             .save()
    )

    df.unpersist()


def main():
    spark = spark_init()
    result_df = get_output_df(spark, load_restaurants_stream(spark))

    (result_df.writeStream.foreachBatch(save_data)
              .option("checkpointLocation", "stream_dir")
              .start().awaitTermination()
    )

if __name__ == "__main__":
    main()
