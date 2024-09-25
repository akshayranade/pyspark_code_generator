"""
This module provisions various data read methods from various data sources.
It reads the data from various sources like jdbc databases, hudi based data warehouse, object stores, etc.
"""
#Import neccessary packages
import pyspark
from pyspark.sql import SparkSession, DataFrame
import logging

# Configure logging for the module
logger = logging.getLogger(__name__)

class DataReader():
    """
    DataReader is a class that provisions various methods to read the data from different data sources.
    
    Attributes:
    source: JSON configuration listing all datasets to read and the connection configurations.
    spark: Spark session object for running pySpark operations.
    output_data: Dictionary to hold intermediate and final dataframes for each pipeline stage.
    """
    
    def __init__(self, source, spark):
        """
        Initializes the DataReader class.
        
        Args:
        source: JSON object defining the datasets to read and connection configurations.
        spark: Spark session to run pySpark operations.
        """
        self.source = source
        self.spark = spark
        self.output_data = {}      # Dictionary to store all read dataframes
        
    def data_read(self) -> dict:
        """
        Method to read all the dataframes various data sources.
        
        Iterates through each source of data, connects to it and reads all the tables.
        
        Returns:
        output_data: Dictionary of all datasets which are read.
        """
        source_dict = self.source
        tables_to_read = source_dict['input_tables'] # Get pall datasets to be read from the config

        for data_source in tables_to_read.keys():
            if 'postgres' in data_source:
                if bool(source_dict["input_tables"].get(data_source)):
                    try:
                        source_conf = source_dict['db_conf'][data_source]
                    except:
                        logger.error("Connection configurations not provided for data source - %s", data_source)

                    #Call read jdbc method to read all tables from given postgres source
                    self.read_from_jdbc(tables=source_dict["input_tables"].get(data_source), conf=source_conf, source_type='postgres')   

            if 'local' in data_source:
                if bool(source_dict["input_tables"].get(data_source)):
                    source_conf = {}
                    self.read_from_object_store(tables=source_dict["input_tables"].get(data_source), conf=source_conf, source_type='local')
                        

            if 'minio' in data_source:
                if bool(source_dict["input_tables"].get(data_source)):
                    try:
                        source_conf = source_dict['db_conf'][data_source]
                    except:
                        logger.error("Connection configurations not provided for data source - %s", data_source)
                    
                    #Call read jdbc method to read all tables from given postgres source
                    self.read_from_object_store(tables=source_dict["input_tables"].get(data_source), conf=source_conf, source_type='minio')

            if 'dwh' in data_source:
                if bool(source_dict["input_tables"].get(data_source)):
                    try:
                        source_conf = source_dict['db_conf'][data_source]
                    except:
                        logger.error("Connection configurations not provided for data source - %s", data_source)
                    
                    #Call read jdbc method to read all tables from given postgres source
                    self.read_from_warehouse(tables=source_dict["input_tables"].get(data_source), conf=source_conf, source_type='dwh')

        return self.output_data


    def read_from_jdbc(self, tables:dict, conf:dict, source_type:str):
        """
        This method reads the data from a given jdbc connection. Used to read from databases like postgres, teradata, etc.

        Args:
            tables: Dictionary of tables to be read.
            conf (dict): configuration to connect to the jdbc data source.
            source_type: source name
        """
        if source_type == 'postgres':
            #Read the connection configurations
            db_url = conf['url']
            db_properties = {
                'user': conf['user'],
                "password": conf['password'],
                "driver": "org.postgresql.Driver"
                }

            #Read the tables and append in self.output_data
            for input_table in tables:
                db_name = tables.get(input_table).split('.')[0]
                table_name = tables.get(input_table).split('.')[1] + '.' + tables.get(input_table).split('.')[2]
                
                db_connect_url = db_url + '/' + db_name
                
                df = self.spark.read.jdbc(url=db_connect_url, table=table_name, properties=db_properties)
                self.output_data[input_table] = df
        
        else:
            #Placeholder for other jdbc databases
            pass


    def read_from_object_store(self, tables:dict, conf:dict, source_type:str):
        """
        This method reads the data from a given object store. Used to read from object stores like minio, ceph, etc.

        Args:
            tables: Dictionary of tables to be read.
            conf (dict): configuration to connect to the object store data source.
            source_type: source name
        """
        if source_type == 'local':
            #Read the tables and append in self.output_data
            for input_table in tables:
                df = self.spark.read.parquet(tables.get(input_table))
                self.output_data[input_table] = df

        elif source_type == 'minio':
            #Read the connection configurations & set spark parameters
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", conf['url'])
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", conf['user'])
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", conf['password'])
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

            #Read the tables and append in self.output_data
            for input_table in tables:
                df = self.spark.read.parquet('s3a:/' + tables.get(input_table))
                self.output_data[input_table] = df
        
        else:
            #Placeholder for other object stores
            pass


    def read_from_warehouse(self, tables:dict, conf:dict, source_type:str):
        """
        This method reads the data from  the data warehouse. 
        With the current system design, the data warehouse uses minio as file storage and hudi as table format, exposed via hive.

        Args:
            tables: Dictionary of tables to be read.
            conf (dict): configuration to connect to the object store data source.
            source_type: source name
        """
        #Read the connection configurations & set spark parameters
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", conf['filesystem_url'])
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", conf['filesystem_user'])
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", conf['filesystem_password'])
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        #Set metastore URI
        # self.spark.conf.set("hive.metastore.uris", conf['hive_metastore_url'])

        for input_table in tables:
            df = self.spark.read.table(tables.get(input_table))
            self.output_data[input_table] = df
