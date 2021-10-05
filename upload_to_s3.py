import os
import boto3
import glob
import configparser
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType, IntegerType as Int

config = configparser.ConfigParser()
config.read_file(open('config.ini'))

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','SECRET')

def create_spark_session():
    ''' This method creates a new spark session or gets reference 
        to an existing session
    '''
    spark = SparkSession \
        .builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0").\
        enableHiveSupport().getOrCreate()
    return spark

# def create_boto3_session():
#     return boto3.Session()

# session = boto3.Session(
#     aws_access_key_id='AKIAYZSVD6GALHK2PQ4K',
#     aws_secret_access_key='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',
# )

def upload_file(s3_resource, source_file_path, bucket, dest_file_path):
    s3_resource.meta.client.upload_file(
        Filename=source_file_path, 
        Bucket=bucket, 
        Key=dest_file_path
    )

def process_metadata(s3, bucket, output_dir, output_folder):
    data_files = [
        ('.', 'i94_countries.csv')
        ('.', 'i94_ports.csv'), 
        ('.', 'i94_states.csv'), 
        ('.', 'us-cities-demographics.csv'), 
        ('.', 'airport-codes_csv.csv'),
        ('../../data2', 'GlobalLandTemperaturesByCity.csv')
    ]

    for path, filename in data_files:
        upload_file(s3, os.path.join(path, filename), bucket, f"{output_dir}/{output_folder}/{filename}")

def convert_sas_date(days):
    """
    Converts SAS date stored as days since 1/1/1960 to datetime
    :param days: Days since 1/1/1960
    :return: datetime
    """
    if days is None:
        return None
    return datetime.date(1960, 1, 1) + datetime.timedelta(days=days)

def process_immigration_data(spark, output_dir, output_folder):
    """ This method processes the immigration data. It converts 
        date fields to the proper format that can be processed, 
        converts some of the fields to the proper data types and 
        uploads data in S3 as parquet files partitioned by year,
        month and day. 

        The partitioning will help schedule data pipelines on a
        daily basis which will reduce the data size to be 
        processed per run which will speed up each run.

        INPUTS:
            spark: The spark session object
            output_dir: S3 directory (relative to the bucket name)
            output_folder: folder name
    """
    get_sas_date = udf(convert_sas_date, DateType())
    get_sas_day = udf(lambda x: convert_sas_date(x).day, Int())

    for fl in glob.glob(os.path.join('../../data/18-83510-I94-Data-2016/*')):
        df_spark = spark.read.format('com.github.saurfang.sas.spark').load(fl)
        
        df = df_spark.withColumn("arrival_date", get_sas_date(df_spark.arrdate)) \
                .withColumn("departure_date", get_sas_date(df_spark.depdate)) \
                .withColumn("year", df_spark.i94yr.cast(Int())) \
                .withColumn("month", df_spark.i94mon.cast(Int())) \
                .withColumn("arrival_day", get_sas_day(df_spark.arrdate)) \
                .withColumn("i94cit", df_spark.i94cit.cast(Int())) \
                .withColumn("i94res", df_spark.i94res.cast(Int())) \
                .withColumn("i94mode", df_spark.i94mode.cast(Int())) \
                .withColumn("i94bir", df_spark.i94bir.cast(Int())) \
                .withColumn("i94visa", df_spark.i94visa.cast(Int())) \
                .withColumn("biryear", df_spark.biryear.cast(Int()))
        
        print("Writing ", fl)
        df.write.mode("append").partitionBy("year", "month", "arrival_day") \
            .parquet(f"{output_dir}/{output_folder}/immigration.parquet")

def main():
    """ Main method to process, cleanup and upload immigration and related
        metadata in S3
    """
    boto3_session = boto3.Session()
    s3 = boto3_session.resource('s3')
    bucket = 'bucket18051988'
    output_dir = 's3a://bucket18051988'
    output_folder = 'capstone_project'
    spark = create_spark_session()

    process_metadata(s3, bucket, output_dir, output_folder)
    process_immigration_data(spark, output_dir, output_folder)
