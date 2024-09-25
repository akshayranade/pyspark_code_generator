"""
This module is the master pySpark code builder from configuration JSONs.
It processes various pipeline stages based on operations like select, join, filter, etc.,
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
from transform.manipulation import Manipulate
from transform.aggregation import Aggregate
from transform.combination import Combine
from transform.selection import Select
from transform.transformation import Transform

# Configure logging for the module
logger = logging.getLogger(__name__)

class CodeMaster():
    """
    CodeMaster is a class that builds and runs a pySpark pipeline based on a configuration file (JSON format).
    
    Attributes:
    pipeline: JSON configuration for defining pipeline stages and operations.
    data: Input data passed to the pipeline.
    spark: Spark session object for running pySpark operations.
    output_data: Dictionary to hold intermediate and final dataframes for each pipeline stage.
    """
    
    def __init__(self, pipeline, input_data, spark):
        """
        Initializes the CodeMaster class.
        
        Args:
        pipeline: JSON object defining the pipeline operations.
        data: Data to be processed by the pipeline.
        spark: Spark session to run pySpark operations.
        """
        self.pipeline = pipeline
        self.input_data = input_data
        self.spark = spark
        self.output_data = {}      # Dictionary to store dataframes generated at each stage
        
    def build_pipeline(self):
        """
        Method to build and execute the pipeline defined in the configuration.
        
        Iterates through each stage of the pipeline and applies transformations like 
        select, join, filter, etc., based on the pipeline JSON.
        
        Returns:
        output_df: The final output dataframe after processing all stages.
        output_data: Dictionary of all intermediate and final dataframes.
        """
        pipeline_dict = self.pipeline
        data_pipeline = pipeline_dict['pipeline'] # Get pipeline stages from the config

        #Create objects for all the independent classes
        data_manipulation_obj = Manipulate(spark=self.spark)
        data_aggregation_obj = Aggregate(spark=self.spark)
        data_transformation_obj = Transform(spark=self.spark)
        data_selection_obj = Select(spark=self.spark)
        data_combination_obj = Combine(spark=self.spark)
        
        # Iterate through all the stages in the pipeline
        for stage in data_pipeline.keys():
            stage_oper = data_pipeline.get(stage) # Get operations in the current stage

            #For every operation, combine the input data and output data to have all the dataframes in the pipeline
            data_dictionary = self.input_data | self.output_data

            # Iterate through operations in each stage (select, join, filter, etc.)
            for oper in stage_oper.keys():
                # Apply 'select' operation
                if oper == 'select':
                    oper_details = stage_oper.get(oper)
                    df_sel = data_selection_obj.selection(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_sel
                        output_df = df_sel
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                # Apply 'join' operation
                elif oper == 'join':
                    oper_details = stage_oper.get(oper)
                    df_join = data_combination_obj.join(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_join
                        output_df = df_join
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                # Apply 'filter' operation
                elif oper == 'filter':
                    oper_details = stage_oper.get(oper)
                    df_fil = data_selection_obj.filter(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_fil
                        output_df = df_fil
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                 # Apply 'union' operation        
                elif oper == 'union':
                    oper_details = stage_oper.get(oper)
                    df_union = data_combination_obj.union(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_union
                        output_df = df_union
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                # Apply 'aggregate' operation        
                elif oper == 'aggregate':
                    oper_details = stage_oper.get(oper)
                    df_aggregate = data_aggregation_obj.aggregate(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_aggregate
                        output_df = df_aggregate
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                # Apply 'pivot' operation
                elif oper == 'pivot':
                    oper_details = stage_oper.get(oper)
                    df_pivot = data_aggregation_obj.pivot(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_pivot
                        output_df = df_pivot
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                # Apply 'unpivot' operation
                elif oper == 'unpivot':
                    oper_details = stage_oper.get(oper)
                    df_unpivot = data_aggregation_obj.unpivot(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_unpivot
                        output_df = df_unpivot
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                # Apply 'rank' operation
                elif oper == 'rank':
                    oper_details = stage_oper.get(oper)
                    df_rank = data_aggregation_obj.rank(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_rank
                        output_df = df_rank
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                # Apply 'switch' operation        
                elif oper == 'switch':
                    oper_details = stage_oper.get(oper)
                    df_switch = data_transformation_obj.switch(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_switch
                        output_df = df_switch
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                # Apply 'string manipulations' operation        
                elif oper == "string_manipulations":
                    oper_details = stage_oper.get(oper)
                    df_string = data_manipulation_obj.string_manipulations(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_string
                        output_df = df_string
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                 # Apply 'datetime functions' operation
                elif oper == "datetime_functions":
                    oper_details = stage_oper.get(oper)
                    df_date_time = data_manipulation_obj.datetime_functions(data=data_dictionary, details=oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.output_data[dataframe_key] = df_date_time
                        output_df = df_date_time
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()
                        
                 # Apply 'change data types' operation       
                elif oper == "change_data_types":
                        oper_details = stage_oper.get(oper)
                        df_change_data_types = data_manipulation_obj.change_data_types(data=data_dictionary, details=oper_details)
                        try:
                            dataframe_key = oper_details['output']
                            self.output_data[dataframe_key] = df_change_data_types
                            output_df = df_change_data_types
                        except:
                            logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                            exit()
                            
                # Apply 'rename columns' operation
                elif oper == "rename_columns":
                        oper_details = stage_oper.get(oper)
                        df_rename_columns = data_manipulation_obj.rename_columns(data=data_dictionary, details=oper_details)
                        try:
                            dataframe_key = oper_details['output']
                            self.output_data[dataframe_key] = df_rename_columns
                            output_df = df_rename_columns
                        except:
                            logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)   
                            exit()
        
        # Return the final output dataframe and dictionary of all dataframes
        return output_df, self.output_data        
