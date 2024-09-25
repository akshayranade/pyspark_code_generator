"""
This module provisions various data write methods from various data sources.
It writes the data into various sources like jdbc databases, hudi based data warehouse, object stores, etc.
"""
#Import neccessary packages
import pyspark
from pyspark.sql import SparkSession, DataFrame
import logging

# Configure logging for the module
logger = logging.getLogger(__name__)

class DataWriter():
    """
    DataWriter is a class that provisions various methods to write the data into different data sources with user provided details.
    
    Attributes:
    source: JSON configuration listing all datasets to write and the connection configurations.
    spark: Spark session object for running pySpark operations.
    input_data: Dictionary to of all dataframes from which certain mentioned dataframes need to be written.
    """
    
    def __init__(self, source, input_data, spark):
        """
        Initializes the DataWriter class.
        
        Args:
        source: JSON object defining the datasets to read and connection configurations.
        spark: Spark session to run pySpark operations.
        """
        self.source = source
        self.spark = spark
        self.input_data = input_data
        
    def data_write(self):
        """
        Method to write all the dataframes to various data sources as mentioned by the user.
        
        Iterates through each data frame to write, connects to the corresponding data source and writes all the tables.
        """
        source_dict = self.source
        tables_to_write = source_dict['output_tables'] # Get pall datasets to be written from the config

        for data_source in tables_to_write.keys():
            if 'postgres' in data_source:
                if bool(source_dict["output_tables"].get(data_source)):
                    try:
                        source_conf = source_dict['db_conf'][data_source]
                    except:
                        logger.error("Connection configurations not provided for data source - %s", data_source)

                    #Call read jdbc method to read all tables from given postgres source
                    self.write_into_jdbc(tables=source_dict["output_tables"].get(data_source), conf=source_conf, source_type='postgres')   

            if 'local' in data_source:
                if bool(source_dict["output_tables"].get(data_source)):
                    source_conf = {}
                    self.write_into_object_store(tables=source_dict["output_tables"].get(data_source), conf=source_conf, source_type='local')
                        

            if 'minio' in data_source:
                if bool(source_dict["output_tables"].get(data_source)):
                    try:
                        source_conf = source_dict['db_conf'][data_source]
                    except:
                        logger.error("Connection configurations not provided for data source - %s", data_source)
                    
                    #Call read jdbc method to read all tables from given postgres source
                    self.write_into_object_store(tables=source_dict["output_tables"].get(data_source), conf=source_conf, source_type='minio')

            if 'dwh' in data_source:
                if bool(source_dict["output_tables"].get(data_source)):
                    try:
                        source_conf = source_dict['db_conf'][data_source]
                    except:
                        logger.error("Connection configurations not provided for data source - %s", data_source)
                    
                    #Call read jdbc method to read all tables from given postgres source
                    self.write_into_warehouse(tables=source_dict["output_tables"].get(data_source), conf=source_conf, source_type='dwh')


    def write_into_jdbc(self, tables:dict, conf:dict, source_type:str):
        """
        This method writes the data into a given jdbc connection. Used to write into databases like postgres, teradata, etc.

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

            #Write the tables
            for output_table in tables:
                df = self.input_data[output_table]
                output_table_dtls = tables.get(output_table)
                
                try:
                    db_name = output_table_dtls.get('db_name')
                    table_string_in_database =  output_table_dtls.get('schema') + '.' + output_table
                except:
                    logger.error("Database name or schema name not provided for output table - %s to write", output_table)
                    
                try:
                    write_mode = output_table_dtls.get('mode')
                except:
                    logger.error("Data Write mode not provided for output table - %s to write", output_table)
                    
                db_connect_url = db_url + '/' + db_name
                df.write.jdbc(url=db_connect_url, table=table_string_in_database, mode=write_mode, properties=db_properties)
        
        else:
            #Placeholder for other jdbc databases
            pass


    def write_into_object_store(self, tables:dict, conf:dict, source_type:str):
        """
        This method writes the data into a given object store. Used to write into object stores like minio, ceph, etc.

        Args:
            tables: Dictionary of tables to be written.
            conf (dict): configuration to connect to the object store data source.
            source_type: source name
        """
        if source_type == 'local':
            #Write the tables
            for output_table in tables:
                df = self.input_data[output_table]
                output_table_dtls = tables.get(output_table)
                
                try:
                    file_location = output_table_dtls.get('file_location')
                except:
                    logger.error("File location not provided for output table - %s to write", output_table)
                    
                try:
                    write_mode = output_table_dtls.get('mode')
                except:
                    logger.error("Data Write mode not provided for output table - %s to write", output_table)
                    
                if output_table_dtls.get("partition_by"):
                    partition_column = output_table_dtls.get('partition_by')
                    df.write.mode(write_mode).partitionBy(partition_column).parquet(file_location)
                else:
                    df.write.mode(write_mode).parquet(file_location)

        elif source_type == 'minio':
            #Read the connection configurations & set spark parameters
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", conf['url'])
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", conf['user'])
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", conf['password'])
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            self.spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

            #Write the tables
            for output_table in tables:
                df = self.input_data[output_table]
                output_table_dtls = tables.get(output_table)
                
                try:
                    file_location = output_table_dtls.get('file_location')
                except:
                    logger.error("File location not provided for output table - %s to write", output_table)
                    
                try:
                    write_mode = output_table_dtls.get('mode')
                except:
                    logger.error("Data Write mode not provided for output table - %s to write", output_table)
                    
                if output_table_dtls.get("partition_by"):
                    partition_column = output_table_dtls.get('partition_by')
                    df.write.mode(write_mode).partitionBy(partition_column).parquet('s3a:/' + file_location)
                else:
                    df.write.mode(write_mode).parquet('s3a:/' + file_location)
        
        else:
            #Placeholder for other object stores
            pass


    def write_into_warehouse(self, tables:dict, conf:dict, source_type:str):
        """
        This method writes the data into the data warehouse. 
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

        for output_table in tables:
            df = self.input_data[output_table]
            output_table_dtls = tables.get(output_table)
            
            hudi_options = {
                'hoodie.table.name': output_table,
                'hoodie.datasource.write.operation': 'insert',
                'hoodie.datasource.write.table.name': output_table,
                "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
                'hoodie.upsert.shuffle.parallelism': '2',
                'hoodie.insert.shuffle.parallelism': '2',
                "hoodie.metadata.enable": "true",
                "hoodie.metadata.index.column.stats.enable": "true",
                "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
                "hoodie.datasource.hive_sync.metastore.uris": conf['hive_metastore_url'],
                "hoodie.datasource.hive_sync.mode": "hms",
                "hoodie.datasource.hive_sync.enable": "true",
                "hoodie.datasource.hive_sync.table": output_table
            }
            
            try:
                database_name = output_table_dtls.get('db_name')
                hudi_options["hoodie.datasource.hive_sync.database"] = database_name
            except:
                logger.error("Database name not provided for output table - %s to write", output_table)
                
            try:
                write_mode = output_table_dtls.get('mode')
            except:
                logger.error("Data Write mode not provided for output table - %s to write", output_table)
                
            if output_table_dtls.get("partition_by"):
                partition_column = output_table_dtls.get('partition_by')
                hudi_options['hoodie.datasource.write.partitionpath.field'] = partition_column
                hudi_options['hoodie.datasource.write.hive_style_partitioning'] = "true"
                hudi_options['hoodie.datasource.write.precombine.field'] = partition_column
                
            else:
                df.write.mode(write_mode).parquet('s3a:/' + file_location)
                
            df.write.format("org.apache.hudi").options(**hudi_options).mode(write_mode).save(f's3a://warehouse/{database_name}/{output_table}')
