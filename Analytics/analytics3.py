from pyspark.sql.functions import *
from pyspark.sql.types import *
from Core.base_driver import BaseDriver

"""
Analytics 3: Which state has highest number of accidents in which females are involved? 
"""

class Analytics3(BaseDriver):
    """
    This is a class for Analytics 3 problem which inherits BaseDriver Class
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

        #Filtering data where Gender is 'female'
        df_female_accident = df_primary_person.filter(lower(col("PRSN_GNDR_ID")) == "female")
        
        #Grouping states and gender and using count operation
        df_agg_female_accident = df_female_accident.groupBy("PRSN_GNDR_ID","DRVR_LIC_STATE_ID").count()

        #Getting a list of city name where the female accidnets are highest
        city_name = df_agg_female_accident.sort(col("count"),ascending=False).collect()[0][1]

        #Filtering on the city name
        df_agg_female_accident = df_agg_female_accident.filter(col("DRVR_LIC_STATE_ID") == city_name)
        
        #Calling BaseDriver function 'write_path' to write the final output to the desired location
        BaseDriver.write_file(df_agg_female_accident, write_path, write_format)