from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark import SparkConf
from os import getenv
def config_load(conf,config_path,config_argument):
    """
    Function to Load the config file 
    """

    #Create the SparkSession
    spark = SparkSession.builder.config(conf=conf).getOrCreate() 

    #Read the config files
    config_path_df = spark.read.format("csv").option("header", True).load(config_path)
    config_argument_df = spark.read.format("csv").option("header", True).load(config_argument)

    temp=[]
    for rows in config_argument_df.collect():
        row_dict = rows.asDict()
        if(row_dict["File_Names"].find(";") == -1):
            config_read_paths = config_path_df.filter(col("File_Names") == row_dict["File_Names"])
            row_dict["Files_Read_Path"] = config_read_paths.collect()[0]["Files_Read_Path"]
            row_dict["File_Read_Format"] = config_read_paths.collect()[0]["File_Read_Format"]
            temp.append(row_dict)
        else:
            temp_str = ""
            for files in row_dict["File_Names"].split(";"):
                config_read_paths = config_path_df.filter(col("File_Names") == files)
                temp_str += config_read_paths.collect()[0]["Files_Read_Path"].strip()+";"
                row_dict["File_Read_Format"] = config_read_paths.collect()[0]["File_Read_Format"]
            
            row_dict["Files_Read_Path"] = temp_str[:-1]
            temp.append(row_dict)

    config_df=spark.createDataFrame(data=temp)
    return config_df