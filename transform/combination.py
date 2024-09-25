"""
This module is to carry out all the operations to combine multiple dataframes like join, union, etc.
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

class Combine():
    """
    Combine is a class that builds and runs a pySpark pipeline based on a configuration file (JSON format).
    
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

    def union(self, data:dict, details:dict) -> DataFrame:
        """
        This method performs a union operation on two dataframes. 
        It can either perform a regular union or a distinct union based on the input parameters.

        Args:
        details (dict): A dictionary containing the details for the union operation, including 
                        left_table_to_union, right_table_to_union, and union_distinct.
        data (dict): Dictionary of all the dataframes in the pipeline

        Returns:
        DataFrame: A dataframe after performing the union operation.
        """
        # Get the data dictionary
        data_dictionary = data

        try:
            # Retrieve the left and right tables to union from the dictionary
            left_table_to_union = data_dictionary[details["left_table_to_union"]]
            right_table_to_union = data_dictionary[details["right_table_to_union"]]
            
        except:
             # Log an error if the table details are missing and exit the program
            logger.error("No dataframe mentioned to select.")
            exit()
            
        # Check if 'union_distinct' is specified and handle accordingly
        if 'union_distinct' in details.keys():
            if details['union_distinct'].upper() == "TRUE":
                # Perform a distinct union if union_distinct is true
                df_unioned = left_table_to_union.union(right_table_to_union).distinct()
            else:
                # Perform a regular union
                 df_unioned = left_table_to_union.union(right_table_to_union)
        return df_unioned
    
    def join(self, data:dict, details: dict) -> DataFrame:
        """
        This method performs a series of join operations on dataframes based on the provided details. 
        It handles multiple join types, selects columns, and applies conditions.

        Args:
        details (dict): A dictionary containing the join operation details, including the left table, 
                        join types, conditions, and any filters or column selections.
        data (dict): Dictionary of all the dataframes in the pipeline

        Returns:
        DataFrame: The resulting dataframe after applying all joins and filters.
        """
        # Get the data dictionary
        data_dictionary = data
    
        try:
            # Retrieve the base table (left_table) from which to start the join operations
            left_table_name = details["left_table"]
            left_table = data_dictionary[left_table_name] 
            
            # Set an alias for the left table for clarity in join operations
            left_table = left_table.alias(left_table_name)
    
            # Process each join specified in the 'joins' section of the details
            for join_detail in details["joins"]:
                # Get the join type (e.g., inner, left, right, etc.)
                join_type = join_detail["type"].lower()
                
                # Get the name of the right table to join with
                right_table_name = join_detail["table"]
                right_table = data_dictionary[right_table_name] # Fetch the dataframe for the right table
                
                # Set an alias for the right table for clarity in join operations
                right_table = right_table.alias(right_table_name)
                
                # Define the join condition using the specified SQL-like expression
                join_condition = F.expr(join_detail["on_condition"])
                
                # Perform the join operation (left_table joins right_table on join_condition)
                left_table = left_table.join(right_table, join_condition, how=join_type)
                
            # If 'select_columns' is specified, select only the required columns
            if "select_columns" in details:
                left_table = left_table.select(details["select_columns"])
                
            # If a 'where_condition' is specified, apply the filter condition to the joined dataframe
            if "where_condition" in details:
                where_condition = F.expr(details["where_condition"])
                left_table = left_table.filter(where_condition)
                
            # Assign the final joined dataframe to df_joined
            df_joined = left_table

            # Return the final dataframe after the join and any other operations
            return df_joined
    
        except KeyError as e:
            # Log an error if a dataframe specified in the details dictionary is not found
            logger.error(f"DataFrame not found: {str(e)}")
            raise
        except Exception as e:
            # Log any other errors that occur during the join operation
            logger.error(f"Error during join operation: {str(e)}")
            raise