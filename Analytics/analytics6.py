from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from Core.base_driver import BaseDriver

"""
Analytics 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols
             as the contributing factor to a crash (Use Driver Zip Code)
"""

class Analytics6(BaseDriver):
    """
    This is a class for Analytics 6 problem which inherits BaseDriver Class
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
        df_primary_person = self.get_raw_file(name[0])
        df_units = self.get_raw_file(name[1])

        #Checking if the files exist, if not raise Exception
        if df_primary_person is None:
            raise Exception(f"no input file of name {name[0]} found")

        if df_units is None:
            raise Exception(f"no input file of name {name[1]} found")
        
        #Left join Primary_Person and Units dataset on CRASH_ID
        df_person_units = df_primary_person.join(df_units,["CRASH_ID"],"left")

        #Concatinating all the contributing factor into one column 'Contribution_combined'
        df_person_units = df_person_units.withColumn("Contribution_combined",concat(col("CONTRIB_FACTR_1_ID"),\
                                                                                lit(","),\
                                                                                col("CONTRIB_FACTR_2_ID"),\
                                                                                lit(","),\
                                                                                col("CONTRIB_FACTR_P1_ID")))

        #Selecting the necessary columns 
        #and filtering on Contribution_combined where there is alcohol or drinking in the text
        df_person_units = df_person_units.select("DRVR_ZIP","Contribution_combined")\
                                        .filter((lower(col("Contribution_combined")).contains("alcohol")) \
                                                | lower(col("Contribution_combined")).contains("drinking"))
        
        #Grouping on Zip column and ranking it on descending order of count
        df_person_units.groupBy("DRVR_ZIP").count()
        windowSpec = Window.orderBy(desc("DRVR_ZIP"))
        df_person_units = df_person_units.withColumn("rank",dense_rank().over(windowSpec))

        #Filtering only top 5 Zip codes(Or States) where alcohol was a contributing factor
        df_person_units = df_person_units.filter(col("rank") <= 5)

        #Calling BaseDriver function 'write_path' to write the final output to the desired location
        BaseDriver.write_file(df_person_units, write_path, write_format)