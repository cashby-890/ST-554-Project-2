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
        self.data = data
        
    # First class method to read in a csv file
    @classmethod
    def read_csv(cls, spark, url: str):
        data = spark.read.load(url, format = "csv", header = True, inferSchema = True)
        return cls(data)
        
    # Second class method to read in a pandas dataframe
    @classmethod
    def read_pandas(cls, spark, pandas_data: DataFrame):
        data = spark.createDataFrame(pandas_data)
        return cls(data)

    # Below are a few validation methods for numeric and string columns.
    # First, we'll check to see if any values are in a user-specified range for a numeric column.
    def value_check(self,col: str, lower: float = None, upper: float = None):
        data = self.data
        # We need to make sure that the column in question is indeed numeric.
        if data.dtypes == [int, float]:
            self = data.withColumn(f"{col}_value_check",data.column.between(lower,upper)) 
            return self.data.show()
        else: 
            print("ERROR! This is not a numeric column!")
            return self.data.show()
        # Since some numeric columns may contain missing values, we'll replace any missing values with NULL.
        if data.filter(data.column.isnull()) == True:
            return NULL
        # If done correctly, this will return the dataframe with an extra column consisting of Boolean values.

    # Next is a function the checks to see if a set of user-specified levels is in a string column.
    def level_check(self,col: str,levels: list):
        data = self.data
        # Again, we need to verify that the column is, in fact, a string column.
        if data.dtypes == [str, object]:
            self = data.withColumn(f"{col}_level_check",data.column.isin(levels))
            return self.data.show()
        else:
            print("ERROR! This is not a string column!")
            return self.data.show()
        # We'll also need to see if anything in the column is missing here; if so, it'll be replaced with NULL>
        if data.filter(data.column.isnull()) == True:
            return NULL
        # Should return the dataframe with an additional column of Boolean values.
        
    # One more validation method we need is to see if any missing values exist in a specified column, regardless of the type.
    def missing_check(self,col: str):
        data = self.data
        # Since we're not worried about the type of column here, conditional logic is not necessary.
        self = data.withColumn(f"{col}_missing",data.column.isnull("col"))
        return self.data.show()
        # As before, it should return the data frame with an appended column of Boolean values.
        
    # To wrap things up, a couple of summarization methods!
    def extreme_values(self,col: str or list,groupby: str = None):
        data = self.data
        # This first function will determine the minimum and maximum values of a specified column or set of columns.
        # As done many a time, we need to see if what's specified is indeed numeric.
        if data.dtypes == [int, float]:
            self = data.select("col").groupBy("groupby").agg(['min','max'])
            return self.data.show()
            # What if only one of the above arguments was inputted?
            if col == None:
                self = data.groupBy("groupby").agg(['min','max'])
                return self.data.show()
            elif groupby == None:
                self = data.select("col").agg(['min','max'])
                return self.data.show()
            else:
                self = data.select_dtypes(include='number').agg(['min','max']) # I'm confused about this one.
                return self.data.show() 
        #IF a non-grouping string-type column is inputted, we'll get the usual error message below.    
        else:
            print("ERROR! This is not a numeric column!")
            return None
        # No numeric-type requested? No results.
        
    def counted_values(self,col_one: str, col_two: str = None):
        data = self.data
        # Our final summary method will report the counts associated with one or two string columns. 
        # That is, if a string-type column was inputted in the first place. A second string column is optional here.
        if data.dtypes == str:
            self = data.groupBy("col_one").count()
            return self.data.show()
            if str in data.dtypes:
                self = data.groupBy("col_one","col_two").count()
                return self.data.show()
        else:
            print("ERROR! This is not a string column!")