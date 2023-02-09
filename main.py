from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf
from Analytics.analytics1 import Analytics1
from Analytics.analytics2 import Analytics2
from Analytics.analytics3 import Analytics3
from Analytics.analytics4 import Analytics4
from Analytics.analytics5 import Analytics5
from Analytics.analytics6 import Analytics6
from Analytics.analytics7 import Analytics7
from Analytics.analytics8 import Analytics8
import Core.config_load as config_load
from os import getenv
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), 'Config', '.env')
load_dotenv(dotenv_path)

#Dictionary to map arguments to the Class name
driver = {
    'analytics1': Analytics1,
    'analytics2': Analytics2,
    'analytics3': Analytics3,
    'analytics4': Analytics4,
    'analytics5': Analytics5,
    'analytics6': Analytics6,
    'analytics7': Analytics7,
    'analytics8': Analytics8
}

# Configuration Object
conf = SparkConf().setAll(
    [
        ('spark.master', getenv('SPARK_HOST', 'local[1]'),),
        ('spark.app.name', getenv('SPARK_APP_NAME', None),),
        ('spark.executor.memory', getenv('EXECUTOR_MEMORY', '1g'),),
        ('spark.executor.cores', getenv('EXECUTOR_CORES', '1'),),
        ('spark.cores.max', getenv('CORES_MAX', '1'),),
        ('spark.driver.memory',getenv('DRIVER_MEMORY', '1g'),)
    ],
)

##Change the Config path to correct path
#Calling config_load function and passing the spark configuration and config file path
config_path="D:/BCG_CaseStudy/Config/"
config_df = config_load(conf,config_path+"db_config_paths.csv",config_path+"db_config_arguments.csv")

#If command line arguments are passed 
if len(sys.argv) > 1:
    #Looping the arguments and calling the appropriate Class
    for usecase in sys.argv[1:]:
        if usecase in driver:
            #Creating the Class object
            bd = driver[usecase](conf)
            
            #Getting the file paths from the config file for the arugment passed
            config_df_temp = config_df.filter(col("Argument_Name") == usecase)

            #If the argument is present in config file
            if config_df_temp.count() > 0:

                #Creating a list from config table
                config_list = config_df_temp.collect()

                #Checking if multiple files needs to be loaded
                if config_list[0]["File_Names"].find(";") == -1:
                    #Loading the file
                    bd.read_file(config_list[0]["File_Names"], config_list[0]["Files_Read_Path"],\
                                 config_list[0]["File_Read_Format"])

                    file_name = []
                    file_name.append(config_list[0]["File_Names"])
                    
                    #Running the process
                    bd.process(file_name,config_list[0]["File_Write_Path"],config_list[0]["File_Write_Format"])
                else:
                    #Getting all the file names and paths that needs to be loaded 
                    file_name = config_list[0]["File_Names"].strip().split(";")
                    file_paths = config_list[0]["Files_Read_Path"].strip().split(";")

                    #Looping and loading all the files that are required
                    for path_num in range(0,len(file_paths)):
                        bd.read_file(file_name[path_num], file_paths[path_num], config_list[0]["File_Read_Format"])
                    
                    #Running the process
                    bd.process(file_name,config_list[0]["File_Write_Path"],config_list[0]["File_Write_Format"])
            else:
                raise Exception(f"no argument {usecase} found in config file, \
                                Note make sure driver dictionary and config file has same name")
else:
    #Calling all the Classes
    for usecase in driver.keys():
        #Creating the Class object
        bd = driver[usecase](conf)
        
        #Getting the file paths from the config file for the arugment passed
        config_df_temp = config_df.filter(col("Argument_Name") == usecase)

        #If the argument is present in config file
        if config_df_temp.count() > 0:

            #Creating a list from config table
            config_list = config_df_temp.collect()

            #Checking if multiple files needs to be loaded
            if config_list[0]["File_Names"].find(";") == -1:
                #Loading the file
                bd.read_file(config_list[0]["File_Names"], config_list[0]["Files_Read_Path"], \
                            config_list[0]["File_Read_Format"])
                file_name = []
                file_name.append(config_list[0]["File_Names"])

                #Running the process
                bd.process(file_name,config_list[0]["File_Write_Path"],config_list[0]["File_Write_Format"])
            else:
                #Getting all the file names and paths that needs to be loaded 
                file_name = config_list[0]["File_Names"].split(";")
                file_paths = config_list[0]["Files_Read_Path"].split(";")

                #Looping and loading all the files that are required
                for path_num in range(0,len(file_paths)):
                    bd.read_file(file_name[path_num], file_paths[path_num], config_list[0]["File_Read_Format"])

                #Running the process
                bd.process(file_name,config_list[0]["File_Write_Path"],config_list[0]["File_Write_Format"])