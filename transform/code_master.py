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
        self.target_df = {}
        
    def build_pipeline(self):
        pipeline_dict = self.pipeline
        data_pipeline = pipeline_dict['pipeline']

        for stage in data_pipeline.keys():
            stage_oper = data_pipeline.get(stage)
            for oper in stage_oper.keys():
                if oper == 'select':
                    oper_details = stage_oper.get(oper)
                    df_sel = self.selection(oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.target_df[dataframe_key] = df_sel
                        output_df = df_sel
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit(1)
                elif oper == 'join':
                    oper_details = stage_oper.get(oper)
                    df_join = self.join(oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.target_df[dataframe_key] = df_join
                        output_df = df_join
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit(1)
                elif oper == 'filter':
                    oper_details = stage_oper.get(oper)
                    df_fil = self.filter(oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.target_df[dataframe_key] = df_fil
                        output_df = df_fil
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit(1)
                elif oper == 'union':
                    oper_details = stage_oper.get(oper)
                    df_union = self.union(oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.target_df[dataframe_key] = df_union
                        output_df = df_union
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit(1)
                elif oper == 'aggregate':
                    oper_details = stage_oper.get(oper)
                    df_aggregate = self.aggregate(oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.target_df[dataframe_key] = df_aggregate
                        output_df = df_aggregate
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit(1)
                elif oper == 'pivot':
                    oper_details = stage_oper.get(oper)
                    df_pivot = self.pivot(oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.target_df[dataframe_key] = df_pivot
                        output_df = df_pivot
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit(1)
                elif oper == 'unpivot':
                    oper_details = stage_oper.get(oper)
                    df_unpivot = self.unpivot(oper_details)
                    try:
                        dataframe_key = oper_details['output']
                        self.target_df[dataframe_key] = df_unpivot
                        output_df = df_unpivot
                    except:
                        logger.error("Output name not defined for the dataframe in stage - %s, operation - %s", stage, oper)
                        exit(1)
        return output_df, self.target_df


    def selection(self, details:dict) -> DataFrame:
        data_dictionary=self.data | self.target_df
        try:
            df_sel = data_dictionary[details["table_name"]] 
           
        except:
            logger.error("Select Operation - No dataframe mentioned to select.")
            exit(1)

        if 'cols_to_select' in details.keys():
            try:
                df_sel = df_sel.select(*details["cols_to_select"])
            except:
                logger.error('Column selection %s for table %s is incorrect.', str(details["cols_to_select"]), details["table_name"])
                exit(1)
        
        if 'drop' in details.keys():
            df_sel = df_sel.drop(*details["drop"])

        if 'dedup' in details.keys():
            if details['dedup'].upper() == "TRUE":
                df_sel = df_sel.drop_duplicates()

        return df_sel

    def join(self, details:dict) -> DataFrame:
        data_dictionary=self.data | self.target_df
        try:
            left_table = data_dictionary[details["left_table"]]
            right_table= data_dictionary[details["right_table"]]
            join_condition = details["join_condition"]
            join_type = details["join_type"]
        except:
            logger.error("Join Operation - Incorrect parameters set for join.")
            exit(1)

        if 'left_table_cols_to_select' in details.keys():
                left_table = left_table.select(*details["left_table_cols_to_select"])

        for column_name in left_table.columns:
            left_table=left_table.withColumnRenamed(column_name,column_name+"_left")
        
        if 'right_table_cols_to_select' in details.keys():
            right_table = right_table.select(*details["right_table_cols_to_select"])
        
        df_joined = left_table.join(right_table, on= F.expr(' AND '.join(join_condition)), how = join_type)

        for column_name in df_joined.columns:
                df_joined=df_joined.withColumnRenamed(column_name,column_name.replace("_left",""))  
        return df_joined
        
    def filter(self, details:dict) -> DataFrame:
        data_dictionary=self.data | self.target_df
        try:
            df_filter = data_dictionary[details["table_name"]]
            filter_condition = details["filter_condition"]
        except:
            logger.error("Filter Operation - Incorrect parameters set for filter.")
            exit(1)
        df_filter = df_filter.where(filter_condition)
        return df_filter
        
    def union(self, details:dict) -> DataFrame:
        data_dictionary=self.data | self.target_df
        try:
            left_table_to_union = data_dictionary[details["left_table_to_union"]]
            right_table_to_union = data_dictionary[details["right_table_to_union"]]
        except:
            logger.error("Union Operation - Incorrect parameters set for union.")
            exit(1)
        if 'union_distinct' in details.keys():
            if details['union_distinct'].upper() == "TRUE":
                df_unioned = left_table_to_union.union(right_table_to_union).distinct()
            else:
                 df_unioned = left_table_to_union.union(right_table_to_union)
        return df_unioned
        
    def aggregate(self, details:dict) -> DataFrame:
        data_dictionary=self.data | self.target_df
        try:
            df_agg = data_dictionary[details["table_name"]]
        except:
            logger.error("Aggregate Operation - No dataframe mentioned to aggregate on.")
            exit(1)

        if 'group_by_cols' in details.keys() or 'aggregate_functions' in details.keys():
            try:
                df_agg = df_agg.groupBy(*details['group_by_cols']).agg(details['aggregate_functions'])

                for column in df_agg.columns:
                    df_agg = df_agg.withColumnRenamed(column, column.replace("(",'_').replace(')',''))

            except:
                logger.error('Aggregate Operation - No group by cols mentioned to aggregate')
                exit(1)
                
        return df_agg
        
    def pivot(self, details:dict) -> DataFrame:
        data_dictionary=self.data | self.target_df
        try:
            df_pivot = data_dictionary[details["table_name"]]     
        except:
            logger.error("Pivot Operation - No dataframe mentioned to pivot on.")
            exit(1)
        if 'cols_to_pivot' in details.keys() or 'group_by_cols' in details.keys():
            try:
                df_pivot = df_pivot.groupBy(*details['group_by_cols']).pivot(*details['cols_to_pivot'])
            except:
                logger.info('Pivot Operation - No group by cols mentioned to pivot')
        return df_pivot
        
    def unpivot(self, details:dict) -> DataFrame:
        data_dictionary=self.data | self.target_df
        try:
            df_unpivot = data_dictionary[details["table_name"]]
        except:
            logger.error("Unpivot Operation - No dataframe mentioned to unpivot.")
            exit(1)
        if 'static_cols' in details.keys() and 'cols_to_unpivot' in details.keys():
            try:
                df_unpivot = df_unpivot.select(*details['static_cols'],F.expr(details["unpivotExpr"]))
            except:
                logger.info('Unpivot Operation - No group by cols mentioned to unpivot')
        return df_unpivot
    