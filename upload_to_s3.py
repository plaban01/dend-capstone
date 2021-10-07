import os
import boto3
import glob
import configparser
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType, IntegerType as Int, StringType

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

def upload_file(s3_resource, source_file_path, bucket, dest_file_path):
    ''' This method uploads a file to S3

    INPUTS:
        s3_resource: boto3 s3 object
        source_file_path: Path of the file
        bucket: s3 bucket name
        dest_file_path: Path relative to the s3 bucket
    '''
    s3_resource.meta.client.upload_file(
        Filename=source_file_path, 
        Bucket=bucket, 
        Key=dest_file_path
    )

def process_metadata(s3, bucket, output_dir, output_folder):
    ''' This method uploads all the input/metadata files in S3

    INPUTS:
        s3_resource: boto3 s3 object
        bucket: s3 bucket name
        output_dir: s3 bucket path
        output_folder: Path relative to the s3 bucket
    '''
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
    This method convert SAS date to datetime format

    INPUTS:
        days: Days in SAS format (since 1/1/60)

    RETURNS:
        datetime
    """
    if days is None:
        return None
    return datetime.date(1960, 1, 1) + datetime.timedelta(days=days)

def convert_visa_category(i94visa):
    ''' This method returns visa category from the visa type code

    INPUTS:
        i94visa: i94 visa type code

    RETURNS:
        string: Visa type
    '''
    if i94visa==1:
        return "Business"
    elif i94visa==2:
        return "Pleasure"
    elif i94visa==3:
        return "Student"
    else:
        return "Others"

def convert_travel_type(i94mode):
    ''' This method returns mode of travel from the associated code

    INPUTS:
        i94mode: Code representing mode of travel

    RETURNS:
        string: Mode of travel
    '''
    if i94mode==1:
        return "Air"
    elif i94mode==2:
        return "Sea"
    elif i94mode==3:
        return "Land"
    else:
        return "Not reported"

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
    get_visa_category = udf(lambda x: convert_visa_category(x), StringType())
    get_travel_mode = udf(lambda x: convert_travel_type(x), StringType())

    for fl in glob.glob(os.path.join('../../data/18-83510-I94-Data-2016/*')):
        df_spark = spark.read.format('com.github.saurfang.sas.spark').load(fl)
        
        df_spark = df_spark.withColumn("arrival_date", get_sas_date(df_spark.arrdate)) \
             .withColumn("departure_date", get_sas_date(df_spark.depdate)) \
             .withColumn("year", df_spark.i94yr.cast(Int())) \
             .withColumn("month", df_spark.i94mon.cast(Int())) \
             .withColumn("arrival_day", get_sas_day(df_spark.arrdate)) \
             .withColumn("i94cit", df_spark.i94cit.cast(Int())) \
             .withColumn("i94res", df_spark.i94res.cast(Int())) \
             .withColumn("travel_mode", get_travel_mode(df_spark.i94mode)) \
             .withColumn("i94bir", df_spark.i94bir.cast(Int())) \
             .withColumn("visa_category", get_visa_category(df_spark.i94visa)) \
             .withColumn("biryear", df_spark.biryear.cast(Int()))

        df = df_spark.select([
            'year', 'month', 'arrival_day', 'cicid', 'arrdate', 'arrival_date', 'i94cit', 'i94res', 'i94port',
            'travel_mode', 'i94addr', 'depdate', 'departure_date', 'i94bir', 'visa_category', 'biryear', 'gender',
            'airline', 'fltno', 'visatype'
        ])

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
