"""
This module is to carry out all the selection and filtering operations
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

class Select():
    """
    Select is a class that builds and runs a pySpark pipeline based on a configuration file (JSON format).
    
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

    def filter(self, data:dict,  details: dict) -> DataFrame:
        """
        This method applies a filter condition to a specified dataframe based on the provided details.

        Args:
        details (dict): A dictionary containing the filter operation details, including the table name 
                        and the filter condition.
        data (dict): Dictionary of all the dataframes in the pipeline

        Returns:
        DataFrame: The filtered dataframe after applying the specified condition.
        """
        # Get the data dictionary
        data_dictionary = data
        try:
            # Retrieve the dataframe specified by "table_name" from the data dictionary
            df_filter = data_dictionary[details["table_name"]]
            
            # Extract the filter condition from the details dictionary
            filter_condition = details["filter_condition"]
        except:
            # Log an error if there is no table mentioned or if there is an issue with retrieving the dataframe
            logger.error("No table details mentioned to select.")
            exit()
            
        # Apply the filter condition to the dataframe using the 'where' method
        df_filter = df_filter.where(filter_condition)
        # Return the filtered dataframe
        return df_filter
    
    def selection(self, data:dict, details: dict) -> DataFrame:
        """
        This method applies the 'select' operation to a dataframe. It performs column selection,
        column dropping, and deduplication based on the details provided in the pipeline configuration.

        Args:
        details (dict): A dictionary that contains information like the table name, columns to select,
                        columns to drop, and whether deduplication is required.
        data (dict): Dictionary of all the dataframes in the pipeline

        Returns:
        DataFrame: The modified dataframe after applying the specified operations.
        """ 
        # Get the data dictionary
        data_dictionary = data
        
        # Try to retrieve the dataframe specified in the 'table_name' field
        try:
            df_sel = data_dictionary[details["table_name"]]
        except:
            
            # Log an error if the specified table is not found in the data
            logger.error("No dataframe mentioned to select.")
            exit()
            
       # If 'cols_to_select' is specified, try to select those columns
        if 'cols_to_select' in details.keys():
            try:
                # Use select to pick specific columns from the dataframe
                df_sel = df_sel.select(*details["cols_to_select"])
            except:
                 # Log an error if column selection fails
                logger.info('Column selection %s for table %s is incorrect.', str(details["cols_to_select"]), details["table_name"])
                exit()
                
        # If 'drop' is specified, drop the mentioned columns from the dataframe
        if 'drop' in details.keys():
            df_sel = df_sel.drop(*details["drop"])
            
         # If 'dedup' is specified, remove duplicate rows if deduplication is set to TRUE
        if 'dedup' in details.keys():
            if details['dedup'].upper() == "TRUE":
                df_sel = df_sel.drop_duplicates()
                
         # Return the final dataframe after applying selection, drop, and deduplication operations
        return df_sel

