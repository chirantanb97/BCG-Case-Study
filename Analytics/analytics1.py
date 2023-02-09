from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf
from Core.base_driver import BaseDriver

"""
Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
"""

class Analytics1(BaseDriver):
    """
    This is a class for Analytics 1 problem which inherits BaseDriver Class
    """

    def __init__(self, *args, **kwargs):
        """
        Constructor to initialize SparkSession and to load required files
        """
        super().__init__(*args, **kwargs)

    def process(self, name, write_path, write_format):
        """
        A function to process and write the final answer in write_path
        
        Parameters:
        name (list): a list containing the file names for processing
        write_path (str): path where we have to write the solution
        write_format (str): format of the file which we are writing
        """

        #Reading the file in a dataframe
        df_primary_person = self.get_raw_file(name[0])

        #Checking if the file exist, if not raise Exception
        if df_primary_person is None:
            raise Exception(f"no input file of name {name[0]} found")

        #Filtering data where Gender is 'male' and has been killed in accident
        df_male_killed = df_primary_person.filter((lower(col("PRSN_GNDR_ID")) == "male") \
            & (lower(col("PRSN_INJRY_SEV_ID")) == "killed"))
        
        #Casting the 'death_cnt' column to integer and then aggregating it
        df_male_killed = df_male_killed.withColumn("DEATH_CNT",col("DEATH_CNT").cast(IntegerType()))
        df_agg_male_killed = df_male_killed.groupBy("PRSN_GNDR_ID","PRSN_INJRY_SEV_ID").sum("DEATH_CNT")
        
        #Calling BaseDriver function 'write_path' to write the final output to the desired location
        BaseDriver.write_file(df_agg_male_killed, write_path, write_format)
