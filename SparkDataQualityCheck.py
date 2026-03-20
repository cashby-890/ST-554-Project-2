from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

# Now that what we need has been imported, let's create our class.

class SparkDataCheck:
    """
    Performs a quality check on a Spark SQL style data frame
    """
    
    # Initializes the class
    def __init__(self, data: DataFrame):
        self.df = data
        
    # First class method to read in a csv file
    @classmethod
    def read_csv(cls, spark, url: string):
        read_in_data = spark.read.load(url, format = "csv", sep = ",", header = True, inferSchema = True)
        return cls(read_in_data)
        
    # Second class method to read in a pandas dataframe
    @classmethod
    def read_pandas(cls, spark, pandas_data: DataFrame):
        converted_data = spark.CreateDataFrame(pandas_data)
        return cls(converted_data)

    # Coming soon, a few validation methods for numeric and string columns...
    def value_check(col: str, lower: float, upper: float):
        # A function that checks to see if a range of values is in a specified numeric column
        # Any missing values will return as NULL
        # If a non-numeric column is put into the function, an error message will print and return the unmodified dataframe.
        # Will return an appended column of Boolean values
        
    def level_check(col: str,levels: list):
        # A function that checks to see if a set of levels is in a specified string column
        # Again, any missing values will return as NULL
        # If a non-string column is put into the function, an error message will print and return the unmodified dataframe.
        # Will return an appended column of Boolean values
        
    def missing_check(col: str):
        # A function that checks to see if any missing values exist in a specified column.
        # Will return an appended column of Boolean values
        
    # To wrap things up, a couple of summarization methods!
    def extreme_values(col: str,groupby: str):
        # Will report the min and max of a numeric column, after verifying it's indeed numeric, and will include an optional grouping column.
        # If no column is specified, the min and max of all numeric columns will be produced, with opitonal grouping.
        # If a non-numeric column is requested, an error message will be printed and will return None
        
    def counted_values(col: str, col: str):
        # Will report the counts associated with one or two string columns; first argument is required while the second argument is optional.
        # If a non-string column is requested, an error message will be printed out letting us know a numeric column was input.