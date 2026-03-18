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
    def read_pandas(cls, spark, pandas_data):
        converted_data = spark.CreateDataFrame(pandas_data)
        return cls(converted_data)

    # Coming soon, a few validation methods for numeric 