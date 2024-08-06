"""
This module is the master pySpark code builder from configuration jsons
"""

import pyspark
from pyspark.sql import SparkSession, DataFrame
import json
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)

class CodeMaster():
    """
    The class expects one parameter as the pipeline config json file
    """
    def __init__(self, pipeline, data, spark):
        self.pipeline = pipeline
        self.data = data
        self.spark = spark

    def build_pipeline(self):
        pipeline_dict = self.pipeline

        target_df = {}

        data_pipeline = pipeline_dict['pipeline']

        for stage in data_pipeline.keys():
            stage_oper = data_pipeline.get(stage)
            for oper in stage_oper.keys():
                if oper == 'select':
                    oper_details = stage_oper.get(oper)
                    df_sel = self.selection(oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        target_df[dataframe_key] = df_sel
                        output_df = df_sel
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit()

                #Add other operations here
        return output_df, target_df


    def selection(self, details:dict) -> DataFrame:
        try:
            df_sel = self.data[details["table_name"]]
        except:
            logger.error("No dataframe mentioned to select.")
            exit()

        if 'cols_to_select' in details.keys():
            try:
                df_sel = df_sel.select(*details["cols_to_select"])
            except:
                logger.info('Column selection %s for table %s is incorrect.', str(details["cols_to_select"]), details["table_name"])
                exit()
        
        if 'drop' in details.keys():
            df_sel = df_sel.drop(*details["drop"])

        if 'dedup' in details.keys():
            if details['dedup'].upper() == "TRUE":
                df_sel = df_sel.drop_duplicates()

        return df_sel