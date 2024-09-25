"""
This module is to carry out all the aggregate operations like grouping, rank order, pivot, unpivot, etc.
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

class Aggregate():
    """
    Aggregate is a class that builds and runs a pySpark pipeline based on a configuration file (JSON format).
    
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
        
        
    def aggregate(self, data:dict, details:dict) -> DataFrame:
        """
        This method performs aggregation on a dataframe using group by columns and aggregate functions.

        Args:
        details (dict): A dictionary containing the table name, group by columns, and aggregate functions.
        data (dict): Dictionary of all the dataframes in the pipeline

        Returns:
        DataFrame: A dataframe after performing the aggregation.
        """
        # Get the data dictionary
        data_dictionary = data
        try:
            # Retrieve the dataframe to aggregate from the dictionary
            df_agg = data_dictionary[details["table_name"]]
        except:
            # Log an error if the table details are missing and exit the program
            logger.error("No dataframe mentioned to select.")
            exit()
            
        # Check if group by columns and aggregate functions are provided
        if 'group_by_cols' in details.keys() or 'aggregate_functions' in details.keys():
            try:
                 # Perform the group by and aggregation
                df_agg = df_agg.groupBy(*details['group_by_cols']).agg(details['aggregate_functions'])
            except:
                 # Log an error if group by columns are missing and exit
                logger.info('No group by cols mentioned to aggregate')
                exit()
                
        return df_agg
        
    def pivot(self, data:dict, details:dict) -> DataFrame:
        """
        This method performs a pivot operation on a dataframe, transforming data based on specified columns.

        Args:
        details (dict): A dictionary containing the table name, columns to pivot, and group by columns.
        data (dict): Dictionary of all the dataframes in the pipeline

        Returns:
        DataFrame: A dataframe after performing the pivot operation.
        """
        # Get the data dictionary
        data_dictionary = data
        try:
            # Retrieve the dataframe to pivot from the dictionary
            df_pivot = data_dictionary[details["table_name"]]     
        except:
            # Log an error if the table details are missing and exit the program
            logger.error("No dataframe mentioned to select.")
            exit()
            
        # Check if columns to pivot and group by columns are provided
        if 'cols_to_pivot' in details.keys() or 'group_by_cols' in details.keys():
            try:
                # Perform the pivot operation using the provided columns
                df_pivot = df_pivot.groupBy(*details['group_by_cols']).pivot(*details['cols_to_pivot'])
            except:
                # Log an error if group by columns are missing
                logger.info('No group by cols mentioned to pivot')
                
        return df_pivot
        
    def unpivot(self, data:dict, details:dict) -> DataFrame:
        """
        This method performs an unpivot operation, converting columns into rows.

        Args:
        details (dict): A dictionary containing the table name, static columns, and columns to unpivot.
        data (dict): Dictionary of all the dataframes in the pipeline

        Returns:
        DataFrame: A dataframe after performing the unpivot operation.
        """
        # Get the data dictionary
        data_dictionary = data
        
        try:
            # Retrieve the dataframe to unpivot from the dictionary
            df_unpivot = data_dictionary[details["table_name"]]
        except:
            # Log an error if the table details are missing and exit the program
            logger.error("No dataframe mentioned to select.")
            exit()
                
        # Check if static columns and columns to unpivot are provided
        if 'static_cols' in details.keys() and 'cols_to_unpivot' in details.keys():
            try:
                # Perform the unpivot operation using the specified expression
                df_unpivot = df_unpivot.select(*details['static_cols'],F.expr(details["unpivotExpr"]))
            except:
                # Log an error if no columns are mentioned for unpivot
                logger.info('No group by cols mentioned to unpivot')
        return df_unpivot
    
    def rank(self, data:dict, details:dict) -> DataFrame:
        """
        This method applies ranking functions to a dataframe based on partitioning and ordering columns.

        Args:
        details (dict): A dictionary containing the table name, ranking function, partitioning columns, 
                        ordering columns, and the ranking function column.
        data (dict): Dictionary of all the dataframes in the pipeline

        Returns:
        DataFrame: A dataframe with the rank column added based on the specified ranking function.
        """
        # Get the data dictionary
        data_dictionary = data
        
        try:
            # Retrieve the dataframe to rank from the dictionary
            df_rank = data_dictionary[details["table_name"]]
            
            # Retrieve the ranking function and partitioning information
            rank_function = details["rank_function"]
            new_col_name= details["new_col_name"]
            
            # Define the window specification based on partitioning and ordering columns
            w = Window.partitionBy(*details['cols_to_partition']).orderBy(*details['cols_to_order'])
            rank_function_col=details["rank_function_col"]
            
            # Apply the appropriate ranking function based on user input
            if rank_function == "first_value":
                df_rank=df_rank.withColumn(new_col_name,F.first_value(rank_function_col).over(w))
            elif rank_function == "last_value":
                df_rank=df_rank.withColumn(new_col_name,F.last_value(rank_function_col).over(w))
            elif rank_function == "lag":
                df_rank=df_rank.withColumn(new_col_name,F.lag(rank_function_col).over(w))
            elif rank_function == "lead":
                df_rank=df_rank.withColumn(new_col_name,F.lead(rank_function_col).over(w))
            elif rank_function == "dense_rank":
                df_rank=df_rank.withColumn(new_col_name,F.dense_rank().over(w))
            elif rank_function == "rank":
                df_rank=df_rank.withColumn(new_col_name,F.rank().over(w))
            
        except:
            # Log an error if the table details are missing and exit the program
            logger.error("No dataframe mentioned to select.")
            exit()
       
        return df_rank