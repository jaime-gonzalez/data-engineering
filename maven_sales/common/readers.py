import os

from pyspark.sql.types import StringType, DateType,TimestampType, IntegerType,FloatType,BooleanType
from pyspark.sql.functions import col

class sparkFileReader:
    def __init__(self,spark,file_path,schema_data):
        self.spark = spark
        self.file_path = file_path
        self.schema_data = schema_data
    
    def read_file(self,file_format='csv',separator=';',date_format='yy-MM-dd',timestamp_Format='yy-MM-dd HH:mm:ss',json_file_parameters={"encoding": "UTF-8", "mode": "PERMISSIVE"}):
        json_to_spark = {
                            'string':StringType(),
                            'date':DateType(),
                            'timestamp':TimestampType(),
                            'integer':IntegerType(),
                            'float':FloatType(),
                            'bool':BooleanType()
        }

        schema = {}
        for col_name, col_type in self.schema_data.items():
            if col_type in json_to_spark:
                schema[col_name]=json_to_spark[col_type]
            else:
                schema[col_name]=StringType()

        if file_format == 'csv':
            df = self.spark.read.csv(self.file_path, sep=separator, header=True, dateFormat=date_format,timestampFormat=timestamp_Format)
        elif file_format == 'parquet':
            df = self.spark.read.parquet(self.file_path)
        elif file_format == 'json':
            df = self.spark.read.json(self.file_path,json_options=json_file_parameters)
        else:
            file_extension = os.path.splitext(self.file_path)[1]
            raise Exception(f"File format '{file_extension}' not supported.")

        for col_name, data_type in schema.items():
            df = df.withColumn(col_name, col(col_name).cast(data_type))
    
        return df