from pyspark.sql import SparkSession, DataFrame

def create_spark_session(app_name):
    """Creates and returns a SparkSession."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName(app_name) \
        .getOrCreate()
    return spark

def read_csv(spark: SparkSession, file_path: str, infer_schema: bool = True, header: bool = True) -> DataFrame:
    """
    Reads a CSV file into a Spark DataFrame.

    :param spark: The SparkSession object.
    :param file_path: The path to the CSV file.
    :param infer_schema: Boolean to infer the schema (default is True).
    :param header: Boolean to use the first row as header (default is True).
    :return: A Spark DataFrame.
    """
    return spark.read.csv(file_path, inferSchema=infer_schema, header=header)