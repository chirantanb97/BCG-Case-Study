from pyspark.sql.functions import *
from pyspark.sql.types import *
from Core.base_driver import BaseDriver

"""
Analytics 7: Count of Distinct Crash IDs where No Damaged Property was observed 
             and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
"""

class Analytics7(BaseDriver):
    """
    This is a class for Analytics 7 problem which inherits BaseDriver Class
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
        df_damages = self.get_raw_file(name[1])

        #Checking if the files exist, if not raise Exception
        if df_damages is None:
            raise Exception(f"no input file of name {name[1]} found")
            
        if df_units is None:
            raise Exception(f"no input file of name {name[0]} found")

        #Converting the 'VEH_DMAG_SCL_2_ID', 'VEH_DMAG_SCL_1_ID' column with value str to integer
        df_units = df_units.withColumn("Damage_sev2",when(lower(col("VEH_DMAG_SCL_2_ID")).contains("1"),lit("1"))\
                                                    .when(lower(col("VEH_DMAG_SCL_2_ID")).contains("2"),lit("2"))\
                                                    .when(lower(col("VEH_DMAG_SCL_2_ID")).contains("3"),lit("3"))\
                                                    .when(lower(col("VEH_DMAG_SCL_2_ID")).contains("4"),lit("4"))\
                                                    .when(lower(col("VEH_DMAG_SCL_2_ID")).contains("5"),lit("5"))\
                                                    .when(lower(col("VEH_DMAG_SCL_2_ID")).contains("6"),lit("6"))\
                                                    .when(lower(col("VEH_DMAG_SCL_2_ID")).contains("7"),lit("7"))\
                                                    .otherwise(lit("0")))
        df_units = df_units.withColumn("Damage_sev2",col("Damage_sev2").cast(IntegerType()))
        
        df_units = df_units.withColumn("Damage_sev1",when(lower(col("VEH_DMAG_SCL_1_ID")).contains("1"),lit("1"))\
                                                    .when(lower(col("VEH_DMAG_SCL_1_ID")).contains("2"),lit("2"))\
                                                    .when(lower(col("VEH_DMAG_SCL_1_ID")).contains("3"),lit("3"))\
                                                    .when(lower(col("VEH_DMAG_SCL_1_ID")).contains("4"),lit("4"))\
                                                    .when(lower(col("VEH_DMAG_SCL_1_ID")).contains("5"),lit("5"))\
                                                    .when(lower(col("VEH_DMAG_SCL_1_ID")).contains("6"),lit("6"))\
                                                    .when(lower(col("VEH_DMAG_SCL_1_ID")).contains("7"),lit("7"))\
                                                    .otherwise(lit("0")))
        df_units = df_units.withColumn("Damage_sev1",col("Damage_sev1").cast(IntegerType()))
        
        #Finding the max damage level from the two columns 'Damage_sev2' and 'Damage_sev1'
        df_units = df_units.withColumn("Max_sev",when(col("Damage_sev1")>=col("Damage_sev2"),col("Damage_sev1"))\
                                                  .when(col("Damage_sev1")<=col("Damage_sev2"),col("Damage_sev2")))

        #Filtering data where severity is greater than 4
        df_units = df_units.withColumn("Max_sev",col("Max_sev").cast(IntegerType())).filter(col("Max_sev") > 4)

        #Left joining Units with Damages dataset on CRASH_ID
        df_damage_unit = df_units.join(df_damages,["CRASH_ID"],"left")

        #Filtering where Damage to Property is not done and insurance was there
        df_damage_unit = df_damage_unit.filter(isnull(col("DAMAGED_PROPERTY")))\
                                        .filter(lower(col("FIN_RESP_TYPE_ID")).contains("insurance"))

        #Calculating distinct Crash_ID Count and formating the text
        df_damage_unit_count = "Number of crashes with damage severity is greater than 4 and car avails Insurance is {0}"\
                                .format(df_damage_unit.select("CRASH_ID").distinct().count())

        #Calling BaseDriver function 'write_path' to write the final output to the desired location
        BaseDriver.write_file(df_damage_unit_count, write_path, write_format)
