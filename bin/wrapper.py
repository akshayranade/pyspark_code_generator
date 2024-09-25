import json, os, uuid, sys
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SparkConf, HiveContext
from transform import *
from dataio import *

import logging

logger = logging.getLogger(__name__)


def etl(source_dict:dict, pipeline_dict:dict, spark:SparkSession):
    #Read all source tables
    input_data = read_data(source_conf, spark)

    #Run the data pipeline
    logger.info("Calling pipeline method.")
    output_df, output_df_dict = pipeline(pipeline_dict=pipeline_dict, data=input_data, spark=spark)
    logger.info("Pipeline method executed.")

    #Write output tables
    write_data(source_conf=source_dict, target_data=output_df_dict, spark=spark)



def pipeline(pipeline_dict:dict, data:dict, spark:SparkSession):
    #code master
    code_master_obj = CodeMaster(pipeline=pipeline_dict, input_data=data, spark=spark)
    df, df_dict = code_master_obj.build_pipeline()

    return df, df_dict


def write_data(source_conf:dict, target_data:dict, spark:SparkSession):
    # Call data writer object and write the data
    data_writer_obj = DataWriter(source=source_conf, input_data=target_data, spark=spark)
    data_writer_obj.data_write()

def read_data(source_conf:dict, spark:SparkSession) -> dict:
    # Call data reader object to read the data
    data_reader_obj = DataReader(source=source_conf, spark=spark)
    input_data = data_reader_obj.data_read()
   
    return input_data



if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()

    source_json = sys.argv[1]
    pipeline_json = sys.argv[2]

    source_conf = json.loads(source_json)
    pipeline_dict = json.loads(pipeline_json)

    print(source_conf)
    print(pipeline_dict)

    #Read the config
    # with open("conf/source.json") as source_conf:
    #     source_conf = json.loads(source_conf.read())

    # with open("conf/pipeline.json") as pipeline_conf:
    #     pipeline_dict = json.loads(pipeline_conf.read())

    #run the etl
    etl(source_dict=source_conf, pipeline_dict=pipeline_dict, spark=spark)
