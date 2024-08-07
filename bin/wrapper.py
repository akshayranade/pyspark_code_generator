import json, os
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SparkConf, HiveContext
from transform import *

import logging

logger = logging.getLogger(__name__)


def etl(source_dict:dict, pipeline_dict:dict, spark:SparkSession):
    #Read all source tables
    input_data = read_data(source_conf, spark)

    #Run the data pipeline
    logger.info("Calling pipeline method.")
    print("Calling pipeline method.")
    output_df, output_df_dict = pipeline(pipeline_dict=pipeline_dict, data=input_data, spark=spark)
    logger.info("Pipeline method executed.")
    print("Pipeline method executed.")


    #Write output tables
    write_data(source_conf=source_dict, target_data=output_df_dict)



def pipeline(pipeline_dict:dict, data:dict, spark:SparkSession):
    #code master
    code_master_obj = CodeMaster(pipeline=pipeline_dict, data=data, spark=spark)
    df, df_dict = code_master_obj.build_pipeline()

    return df, df_dict


def write_data(source_conf:dict, target_data:dict):
    for output_table in source_conf['output_tables'].keys():
        df = target_data[output_table]
        df.coalesce(1).write.mode('overwrite').parquet(source_conf['output_tables'].get(output_table))


def read_data(source_conf:dict, spark:SparkSession) -> dict:
    input_data = {}
    # for table in source["input_tables"].keys():
    #     df = spark.read.table(source["input_tables"].get(table))
    #     input_data[table] = df

    simpleData = [("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "Sales", 4100),
        ("Maria", "Finance", 3000),
        ("James", "Sales", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3900),
        ("Jeff", "Marketing", 3000),
        ("Kumar", "Marketing", 2000),
        ("Saif", "Sales", 4100)
    ]
    schema = ["employee_name", "department", "salary"]
    
    employee_department = spark.createDataFrame(data=simpleData, schema = schema)

    #Add to input_data dictionary
    input_data['employee_department'] = employee_department

    simpleData = [("James", "Stewart", 11111),
        ("Michael", "Paine", 44444),
        ("Robert", "Baggins", 12345),
        ("Maria", "Hanks", 34567),
        ("James", "Snape", 43215),
        ("Scott", "Potter", 65234),
        ("Jen", "Granger", 76234),
        ("Jeff", "Weasley", 54234),
        ("Kumar", "Oppenheimer", 34567),
        ("Saif", "Tesla", 43215)
    ]
    schema = ["first_name", "last_name", "pin_code"]
    
    employee_demo = spark.createDataFrame(data=simpleData, schema = schema)

    #Add to input_data dictionary
    input_data['employee_demo'] = employee_demo

    return input_data



if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()

    #Read the config
    with open("conf/source.json") as source_conf:
        source_conf = json.loads(source_conf.read())

    with open("conf/pipeline.json") as pipeline_conf:
        pipeline_dict = json.loads(pipeline_conf.read())

    #run the etl
    etl(source_dict=source_conf, pipeline_dict=pipeline_dict, spark=spark)
