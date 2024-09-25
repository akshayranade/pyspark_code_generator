"""
This module is to carry out all the stand-alone data manipulation like string manipulations, data type changes, etc.
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

class Manipulate():
    """
    Manipulate is a class that builds and runs a pySpark pipeline based on a configuration file (JSON format).
    
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

    def string_manipulations(self, data:dict, details: dict) -> DataFrame:
        """
        Performs a series of string manipulations on specified columns of a DataFrame 
        based on the provided details.

        Args:
            details (dict): The string manipulation operation details including the table name, 
                            column name, and a list of operations to apply sequentially.
            data (dict): The dictionnary of all the dataframes in the data pipeline

        Returns:
            DataFrame: The resulting DataFrame after applying the string manipulation operations.
        """
        # Get the data dictionary
        data_dictionary = data
        
        # Retrieve the dataframe to do string manipulations from the dictionary
        try:
            df_string = data_dictionary[details["table_name"]]
        except:
            logger.error("No dataframe mentioned to select.")
            exit()

        # Iterate through the operations to be performed on each column
        for operation in details["operations"]:
            # Get the type of the operation to perform
            operation_type = operation["type"]
            # Retrieve the column on which the operations will be applied
            column=details["column"]
            
            # Perform the operation based on its type
            if operation_type == "replace":
                # Replace occurrences of a pattern in the column with a replacement string
                pattern = operation["params"]["pattern"]
                replacement = operation["params"]["replacement"]
                df_string = df_string.withColumn(operation["modified_col_name"], F.regexp_replace(details["column"], pattern, replacement))
                
            elif operation_type == "lower":
                # Convert the column values to lowercase
                df_string = df_string.withColumn(operation["modified_col_name"], F.lower(details["column"]))
                
            elif operation_type == "upper":
                # Convert the column values to uppercase
                df_string = df_string.withColumn(operation["modified_col_name"], F.upper(details["column"]))
                
            elif operation_type == "ltrim":
                # Remove leading whitespace from the column values
                df_string = df_string.withColumn(details["column"], F.ltrim(details["column"]))
                
            elif operation_type == "rtrim":
                # Remove trailing whitespace from the column values
                df_string = df_string.withColumn(details["column"], F.rtrim(details["column"]))
                
            elif operation_type == "substring":
                # Extract a substring from the column values
                start = operation["params"]["start"]
                length = operation["params"]["length"]
                df_string = df_string.withColumn(operation["modified_col_name"], F.substring(details["column"], start + 1, length))
                
            elif operation_type == "trim":
                # Remove leading and trailing whitespace from the column values
                df_string = df_string.withColumn(details["column"], F.trim(details["column"]))
                
            elif operation_type == "concat":
                # Concatenate a prefix to the column values
                prefix = operation["params"]["prefix"]
                df_string = df_string.withColumn(operation["modified_col_name"], F.concat(F.lit(prefix), details["column"]))
                
            elif operation_type == "reverse":
                # Reverse the characters in the column values
                df_string = df_string.withColumn(operation["modified_col_name"], F.reverse(details["column"]))
                
            elif operation_type == "translate":
                # Translate characters in the column based on source and destination mappings
                src = operation["params"]["src"]
                dest = operation["params"]["dest"]
                df_string = df_string.withColumn(operation["modified_col_name"], F.translate(details["column"], src, dest))
                
            elif operation_type == "contains":
                # Check if the column contains a specific substring
                substring = operation["params"]["substring"]
                df_string = df_string.withColumn(operation["modified_col_name"] + "_contains", F.col(details["column"]).contains(substring))
                
            elif operation_type == "startswith":
                # Check if the column starts with a specific prefix
                prefix = operation["params"]["prefix"]
                df_string = df_string.withColumn(operation["modified_col_name"] + "_startswith", F.col(details["column"]).startswith(prefix))
                
            elif operation_type == "endswith":
                # Check if the column ends with a specific suffix
                suffix = operation["params"]["suffix"]
                df_string = df_string.withColumn(operation["modified_col_name"] + "_endswith", F.col(details["column"]).endswith(suffix))
                
            elif operation_type == "lpad":
                # Left-pad the column values with a specified character up to a given length
                length = operation["params"]["length"]
                pad = operation["params"]["pad"]
                df_string = df_string.withColumn(operation["modified_col_name"], F.lpad(F.col(details["column"]), length, pad))
                
            elif operation_type == "rpad":
                # Right-pad the column values with a specified character up to a given length
                length = operation["params"]["length"]
                pad = operation["params"]["pad"]
                df_string = df_string.withColumn(operation["modified_col_name"], F.rpad(F.col(details["column"]), length, pad))
                
            elif operation_type == "regexp_extract":
                # Extract a substring from the column values using a regular expression pattern
                pattern = operation["params"]["pattern"]
                idx = operation["params"]["idx"]
                df_string = df_string.withColumn(operation["modified_col_name"] + "_regexp_extract", F.regexp_extract(F.col(details["column"]), pattern, idx))
                
            elif operation_type == "regexp_replace":
                # Replace occurrences of a pattern in the column values with a replacement string using regular expressions
                pattern = operation["params"]["pattern"]
                replacement = operation["params"]["replacement"]
                df_string = df_string.withColumn(operation["modified_col_name"], F.regexp_replace(F.col(details["column"]), pattern, replacement))
            
        return df_string

    def change_data_types(self, data:dict, details: dict) -> DataFrame:
        """
        Change data type operations as defined in the details dictionary.
        
        :param details: Dictionary containing details of the datatype operations.
        :param data: Dictionary of dataframes
        :return: DataFrame with datatype operations applied.
        """
        # Get the data dictionary
        data_dictionary = data
    
        try:
            # Get the table name and the DataFrame
            df = data_dictionary[details["table_name"]]
            
            # Define a mapping from string data types to PySpark data types
            type_mapping = {
                "int": IntegerType(),
                "float": FloatType(),
                "double": DoubleType(),
                "date": DateType(),
                "string": StringType()
            }
            
            # Iterate through the columns and change their data types
            for column_detail in details["columns"]:
                # Extract column name and target data type from details
                column_name = column_detail["column"]
                target_type = column_detail["data_type"].lower()
                
                # Check if the specified data type is valid
                if target_type in type_mapping:
                    # Cast the column to the target data type
                    df = df.withColumn(column_name, df[column_name].cast(type_mapping[target_type]))
                else:
                    # Raise an error if the target data type is not supported
                    raise ValueError(f"Data type '{target_type}' for column '{column_name}' is not supported.")
            
            return df
    
        except KeyError as e:
            logger.error(f"Table or column not found: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error during data type conversion: {str(e)}")
            raise
        

    def datetime_functions(self, data:dict, details: dict) -> DataFrame:
        """
        Apply datetime operations as defined in the details dictionary.
        
        :param details: Dictionary containing details of the datetime operations.
        :param data: Dictionary of dataframes
        :return: DataFrame with datetime operations applied.
        """

        # Get the data dictionary
        data_dictionary = data
        
        try:
            # Get the DataFrame from the data dictionary
            df = data_dictionary[details["table_name"]]
            
            # Iterate through the datetime operations
            for operation in details["operations"]:
                # Extract the type of operation and the name of the new column to be created
                op_type = operation["operation"]
                new_column = operation["new_column"]

                # Perform different operations based on the operation type
                if op_type == "current_date":
                    # Add a column with the current date
                    df = df.withColumn(new_column, F.current_date())
                
                elif op_type == "current_timestamp":
                    # Add a column with the current timestamp
                    df = df.withColumn(new_column, F.current_timestamp())
                    
                elif op_type == "date_format":
                    # Format a date column according to a specified format
                    column = operation["column"]
                    fmt = operation["format"]
                    df = df.withColumn(new_column, F.date_format(F.col(column), fmt))
                
                elif op_type == "to_date":
                    # Convert a column to date type based on a specified format
                    column = operation["column"]
                    fmt = operation["format"]
                    df = df.withColumn(new_column, F.to_date(F.col(column), fmt))
                
                elif op_type == "year":
                     # Extract the year from a date column
                    column = operation["column"]
                    df = df.withColumn(new_column, F.year(F.col(column)))
                
                elif op_type == "month":
                     # Extract the month from a date column
                    column = operation["column"]
                    df = df.withColumn(new_column, F.month(F.col(column)))
                
                elif op_type == "add_days":
                    # Add a specified number of days to a date column
                    column = operation["column"]
                    days = operation["days"]
                    df = df.withColumn(new_column, F.date_add(F.col(column), days))
                
                elif op_type == "subtract_days":
                    # Subtract a specified number of days from a date column
                    column = operation["column"]
                    days = operation["days"]
                    df = df.withColumn(new_column, F.date_sub(F.col(column), days))
                
                elif op_type == "datediff":
                     # Calculate the difference in days between two date columns
                    start_date_column = operation["start_date_column"]
                    end_date_column = operation["end_date_column"]
                    df = df.withColumn(new_column, F.datediff(F.col(end_date_column), F.col(start_date_column)))
                
                else:
                    # Raise an error if the operation type is not supported
                    raise ValueError(f"Unsupported datetime operation: {op_type}")
            
            # Return the resulting DataFrame
            return df
            
        except KeyError as e:
            # Log an error if a DataFrame or column is not found in the provided details
            logger.error(f"DataFrame not found: {str(e)}")
            raise
        except Exception as e:
            # Log any other exceptions that occur during datetime operations
            logger.error(f"Error during datetime operations: {str(e)}")
            raise
            
    def rename_columns(self, data:dict, details: dict) -> DataFrame:
        """
        Renames columns in a DataFrame based on the provided mapping from JSON.
    
        :param details: Dictionary containing the table name and column mappings.
        :param data: Dictionary of all dataframes
        :return: DataFrame with renamed columns.
        """
        # Get the data dictionary
        data_dictionary = data
        
        try:
            # Get the DataFrame to be renamed
            df_rename = data_dictionary[details["table_name"]]
            
            # Get the column mappings from the details
            column_mappings = details["column_mappings"]
            
            # Rename columns based on the mapping
            for old_name, new_name in column_mappings.items():
                df_rename = df_rename.withColumnRenamed(old_name, new_name)
            
            return df_rename
        
        except KeyError as e:
            logger.error(f"DataFrame or column not found: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error during column renaming operation: {str(e)}")
            raise