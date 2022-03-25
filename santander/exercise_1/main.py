
# required libraries
import sys
import os
from os.path import join
from os.path import dirname
from dotenv import load_dotenv

import pyspark
from pyspark.sql import types
from pyspark.sql import functions as F

from helpers.helpers import create_spark_session
from helpers.helpers import read_file_with_header
from helpers.helpers import read_file_with_no_header
from helpers.helpers import write_file_to_parquet



def ingest_data(input_path, output_path, delimiter, header, extra_col_name=None, extra_col_value=None):
    """
    Function to ingest a text file with data. Parameters passed should be defined in an .env file
    """
    # create spark session
    spark = create_spark_session()

    if header=='True':
        print("Processing a file with header")

        # read file
        df = read_file_with_header(session=spark, input_path=input_path, delimiter=delimiter, header=header)
        
        # df = df \
        #     .withColumn("transaction_date", F.concat("transaction_date", F.lit(" 00:00:00.000"))) \
        #     .withColumn("transaction_date", F.col("transaction_date").cast(types.TimestampType()))

        # check if extra column is required
        if extra_col_name is not None:
            df = df.withColumn(extra_col_name, F.lit(extra_col_value).cast(types.DateType()))

        # print some records, for debugging only
        df.orderBy(F.col('stock')).show(10)
        df.printSchema()
        
        # write the file to parquet
        # check if there is extra column, we need to repartition by it
        if extra_col_name is not None:
            write_file_to_parquet(df=df, output_path=output_path, repartition_col=extra_col_name)
        else:
            write_file_to_parquet(df=df, output_path=output_path)
    
    elif header=='False':
        print("Processing a file without header")
    
        # read file
        df = read_file_with_no_header(session=spark, input_path=input_path, delimiter=delimiter, header=header)

        # print some records, for debugging only
        df.show(10)
        df.printSchema()
        
        # write the file to parquet
        write_file_to_parquet(df=df, output_path=output_path)


    # stop spark session
    spark.stop()




# main
def main():
    
    # check if script is called with an .env file
    if len(sys.argv) == 1:
        print("Please provide an .env file in your call")
        sys.exit()

    # check if .env file passed is correct    
    if sys.argv[1] == "config_csv.env":
        pass    
    elif sys.argv[1] == "config_csv_ec.env":
        pass
    elif sys.argv[1] == "config_txt.env":
        pass
    else:
        print("Please provide a valid .env file")
        sys.exit()

    # load config file
    dotenv_path = join(dirname(__file__), sys.argv[1])
    load_dotenv(dotenv_path)

    # read env variables
    input_path = os.environ.get('INPUT_PATH') 
    output_path = os.environ.get('OUTPUT_PATH') 
    delimiter = os.environ.get('DELIMITER') 
    header = os.environ.get('HEADER') 
    extra_col_name = os.environ.get('EXTRA_COL_NAME') 
    extra_col_value = os.environ.get('EXTRA_COL_VALUE') 
    
    # print env variables 
    print(f'Input path is: {input_path}')
    print(f'Output path is: {output_path}')
    print(f'Delimiter is: {delimiter}')
    print(f'Header is: {header}')
    print(f'Extra col name is: {extra_col_name}')
    print(f'Extra col value is: {extra_col_value}')

    # call to ingest_data()
    ingest_data(input_path=input_path, output_path=output_path, delimiter=delimiter, 
                header=header, extra_col_name=extra_col_name, extra_col_value=extra_col_value)

    

if __name__ == "__main__":
    main() 