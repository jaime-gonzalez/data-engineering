import sys
import os
import json

script_directory = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_directory,'..', 'common'))

from extractors import zipFileExtractor
from readers import sparkFileReader
from config import schema_file_path,credentials_file_path,jdbc_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def extractAndLoad(output_schema=None,output_table=None,file_name=None):
    
    file_url = "https://maven-datasets.s3.amazonaws.com/Maven+Toys/Maven+Toys+Data.zip"
    file_output_directory=f"/home/jaimegonzalez/Desktop/data-engineering/maven_sales/bucket/{file_name}"
    
    try:
        table_schema = json.load(open(schema_file_path, 'r')).get(output_table, {})

        try:
            credentials = json.load(open(credentials_file_path, 'r'))
            host = credentials.get('host')
            user = credentials.get('user')
            password = credentials.get('password')
            database = credentials.get('database')
            
            try:
                zipFileExtractor(input_url=file_url,output_directory=file_output_directory).download_file(file_name=file_name)
            
                try:

                    spark = SparkSession.builder \
                                .appName("generic_etl") \
                                .config("spark.driver.extraClassPath",jdbc_path) \
                                .config("spark.sql.session.timeZone", "UTC") \
                                .getOrCreate()
                    
                    df = sparkFileReader(spark = spark,file_path=file_output_directory, schema_data=table_schema).read_file(separator=',')
                    df = df.withColumn("insertion_date", current_timestamp())

                    database_url = f"jdbc:postgresql://{host}/{database}"
                    database_properties = {
                            "user": user,
                            "password": password
                    }

                    df.write \
                            .jdbc(
                                url=database_url, 
                                table=f"{output_schema}.{output_table}", 
                                mode="overwrite", 
                                properties=database_properties
                            )
                    
                    spark.stop()

                except:
                    raise(sparkFileReader.__getattribute__(Exception))
            except:
                raise(zipFileExtractor.__getattribute__(Exception))              
        except:
            raise Exception(f"Failed to get credentials from: {credentials_file_path}")
    except:
        raise Exception(f"Schema not found for table: {output_table}")
