import sys
import os

import boto3
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.functions import count, max, min
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

aws_region = "us-east-1"
dynamo_db_tablename = 'iserikov_suspicious_ips'


def get_suspicious_ips(bucket, path):
    path_to_dir = os.path.join("s3a://", bucket, path)
    spark = SparkSession \
        .builder \
        .appName("find_suspicious_ips") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("device_id", StringType()),
        StructField("device_type", StringType()),
        StructField("item_id", StringType()),
        StructField("ts", TimestampType()),
        StructField("user_ip", StringType()),
    ])

    df = spark.read \
        .option("recursiveFileLookup", "true") \
        .option("header", "true") \
        .schema(schema) \
        .json(path_to_dir)

    print('count=',df.count())

    suspicious_ips = df.groupby("user_ip") \
        .agg(count("item_id").alias("number_of_events"),
             (max(col("ts")).cast("long") - min(col("ts")).cast("long")).alias("ts_diff")) \
        .filter(col("number_of_events") / col("ts_diff") > 1).select("user_ip")

    return suspicious_ips


def write_to_dynamo(ips):
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table(dynamo_db_tablename)
    for item in ips.collect():
        table.put_item(
            Item={
                'suspicious_ip': item['user_ip']
            }
        )


if __name__ == "__main__":
    print ('-----------')
    write_to_dynamo(get_suspicious_ips(sys.argv[1], sys.argv[2]))
