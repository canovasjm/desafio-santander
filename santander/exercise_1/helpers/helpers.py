# required libraries
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F


# custom functions
def create_spark_session():
    """
    Function to create the Spark session.

    Returns
    ----------
    spark : spark session
    """

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('santander') \
        .getOrCreate()

    return spark


def read_file_with_header(session, input_path: str, delimiter: str, header: str):
    """
    Function to read a text file with header.

    Parameters
    ----------
    session 
        Spark session.
    input_path : str
        Path to the file to be read.
    delimiter : str
        Field delimiter in the input text file.
    header: str
        Specifies if the file to be read has a header or not. Either `True` or `False`.

    Returns
    ----------
    df : pyspark dataframe
    """

    # define the schema
    schema = types.StructType([
        types.StructField('stock', types.StringType(), True),
        types.StructField('transaction_date', types.StringType(), True),
        types.StructField('open_price', types.FloatType(), True),
        types.StructField('close_price', types.FloatType(), True),
        types.StructField('max_price', types.FloatType(), True),
        types.StructField('min_price', types.FloatType(), True),
        types.StructField('variation', types.FloatType(), True)
    ])

    # read the file as pyspark dataframe
    df = session.read \
        .options(delimiter=delimiter, header=header) \
        .schema(schema) \
        .csv(input_path)

    return df


def read_file_with_no_header(session, input_path: str, delimiter: str, header: str):
    """
    Function to read a text file without header.

    Parameters
    ----------
    session 
        Spark session.
    input_path : str
        Path to the file to be read.
    delimiter : str
        Field delimiter in the input text file.
    header: str
        Specifies if the file to be read has a header or not. Either `True` or `False`.

    Returns
    ----------
    df : pyspark dataframe
    """

    # read the file as pyspark data frame
    df = session.read \
        .options(delimiter=delimiter, header=header) \
        .csv(input_path)

    # split columns by their lenght
    df = df \
        .withColumn('stock', F.substring(df._c0, 1, 6)) \
        .withColumn('transaction_date', F.substring(df._c0, 7, 24)) \
        .withColumn('open_price', F.substring(df._c0, 31, 7)) \
        .withColumn('close_price', F.substring(df._c0, 38, 7)) \
        .withColumn('max_price', F.substring(df._c0, 45, 7)) \
        .withColumn('min_price', F.substring(df._c0, 52, 7)) \
        .withColumn('variation', F.substring(df._c0, 59, 7))
    
    df = df.drop('_c0')

    return df


def write_file_to_parquet(df, output_path: str, repartition_col: str = 'stock', mode: str = 'overwrite'):
    """
    Function to save a PySpark dataframe to parquet.

    Parameters
    ----------
    df : pyspark dataframe
        Pyspark dataframe to be saved as parquet file.
    output_path : str
        Path to the folder where the parquet file is going to be written.
    repartition_col : str
        Name of a column to repartition the pyspark dataframe (default is 'stock')
    mode: str
        Specifies the behavior of the save operation when data already exists. Possible values are:
            'append': Append contents of this DataFrame to existing data.
            'overwrite' (default case): Overwrite existing data.
            'ignore': Silently ignore this operation if data already exists.
            'error' or 'errorifexists': Throw an exception if data already exists.
    """

    # write the file to parquet
    df.repartition(repartition_col).write.parquet(path=output_path, mode=mode)
