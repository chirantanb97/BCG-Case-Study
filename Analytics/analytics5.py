from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from Core.base_driver import BaseDriver

"""
Analytics 5: For all the body styles involved in crashes, 
             mention the top ethnic user group of each unique body style
"""

class Analytics5(BaseDriver):
    """
    This is a class for Analytics 5 problem which inherits BaseDriver Class
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
        
        #Reading the files in a dataframe
        df_units = self.get_raw_file(name[0])
        df_primary_person = self.get_raw_file(name[1])

        #Checking if the files exist, if not raise Exception
        if df_primary_person is None:
            raise Exception(f"no input file of name {name[1]} found")
            
        if df_units is None:
            raise Exception(f"no input file of name {name[0]} found")

        #Left join Units and Primary_Person dataset on CRASH_ID
        df_person_units = df_units.join(df_primary_person,["CRASH_ID"],"left")

        #Grouping on body style and ethnicity
        df_person_units = df_person_units.groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").count()

        #Assigning rank partitioned on body style in decreasing order of count
        windowSpec  = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("count"))
        df_person_units = df_person_units.withColumn("rank",dense_rank().over(windowSpec))

        #Filtering only top ethnic group(rank=1) for unique body style 
        df_person_units = df_person_units.filter(col("rank") == 1).drop("rank")

        #Calling BaseDriver function 'write_path' to write the final output to the desired location
        BaseDriver.write_file(df_person_units, write_path, write_format)