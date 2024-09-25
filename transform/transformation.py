"""
This module is to carry out transformation operations on dataframes like case-when, etc.
as defined in a JSON configuration file.
"""
#Import neccessary packages
import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType, FloatType, DateType, StringType, DoubleType
import json
from pyspark.sql import functions as F
import logging
from pyspark.sql import Window 

# Configure logging for the module
logger = logging.getLogger(__name__)

class Transform():
    """
    Transform is a class that builds and runs a pySpark pipeline based on a configuration file (JSON format).
    
    Attributes:
    spark: Spark session object for running pySpark operations.
    """
    
    def __init__(self, spark):
        """
        Initializes the CodeMaster class.
        
        Args:
        spark: Spark session to run pySpark operations.
        """
        self.spark = spark

    def switch(self, data:dict, details:dict) -> DataFrame:
        """
        This method applies a switch operation to a dataframe by executing a specific SQL expression.

        Args:
        details (dict): A dictionary containing the table name and the SQL expression to apply.
        data (dict): Dictionary of all the dataframes in the pipeline

        Returns:
        DataFrame: A dataframe after applying the switch expression.
        """
        # Get the data dictionary
        data_dictionary = data
        try:
            # Retrieve the dataframe to apply the switch on
            df_switch = data_dictionary[details["table_name"]]
            
            # Apply the provided SQL expression to the dataframe
            df_switch = df_switch.select("*",F.expr(details["expression"]))
            
        except:
             # Log an error if the table details are missing and exit the program
            logger.error("No dataframe mentioned to select.")
            exit()
            
        return df_switch