from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from Core.base_driver import BaseDriver

"""
Analytics 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries
             including death
"""

class Analytics4(BaseDriver):
    """
    This is a class for Analytics 4 problem which inherits BaseDriver Class
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
        df_units = self.get_raw_file(name[0])

        #Checking if the file exist, if not raise Exception
        if df_units is None:
            raise Exception(f"no input file of name {name[0]} found")

        #Casting the columns related to injury count to Integer
        df_units = df_units.withColumn("INCAP_INJRY_CNT",col("INCAP_INJRY_CNT").cast(IntegerType()))\
                            .withColumn("NONINCAP_INJRY_CNT",col("NONINCAP_INJRY_CNT").cast(IntegerType()))\
                            .withColumn("POSS_INJRY_CNT",col("POSS_INJRY_CNT").cast(IntegerType()))\
                            .withColumn("NON_INJRY_CNT",col("NON_INJRY_CNT").cast(IntegerType()))\
                            .withColumn("UNKN_INJRY_CNT",col("UNKN_INJRY_CNT").cast(IntegerType()))\
                            .withColumn("TOT_INJRY_CNT",col("TOT_INJRY_CNT").cast(IntegerType()))\
                            .withColumn("DEATH_CNT",col("DEATH_CNT").cast(IntegerType()))

        #Aggregating the columns based on 'VEH_MAKE_ID'
        df_agg_veh_make_id = df_units.groupBy("VEH_MAKE_ID").sum("INCAP_INJRY_CNT","NONINCAP_INJRY_CNT",\
                                                                "POSS_INJRY_CNT","NON_INJRY_CNT",\
                                                                "UNKN_INJRY_CNT","TOT_INJRY_CNT",\
                                                                "DEATH_CNT")

        #Summing up all the count of various injuries into one column 'Total_Injuries'
        df_agg_veh_make_id = df_agg_veh_make_id.withColumn("Total_Injuries",col("sum(INCAP_INJRY_CNT)")\
                                                                            +col("sum(NONINCAP_INJRY_CNT)")\
                                                                            +col("sum(POSS_INJRY_CNT)")\
                                                                            +col("sum(NON_INJRY_CNT)")\
                                                                            +col("sum(UNKN_INJRY_CNT)")\
                                                                            +col("sum(TOT_INJRY_CNT)")\
                                                                            +col("sum(DEATH_CNT)"))

        #Selecting the columns required and ranking based on Total_Injuries
        df_agg_veh_make_id = df_agg_veh_make_id.select("VEH_MAKE_ID","Total_Injuries")
        windowSpec  = Window.orderBy(desc("Total_Injuries"))
        df_agg_veh_make_id = df_agg_veh_make_id.withColumn("rank",dense_rank().over(windowSpec))

        #Filtering only rank between 5 to 15
        df_agg_veh_make_id=df_agg_veh_make_id.filter((col("rank") >= 5) & (col("rank") <= 15))
        
        #Calling BaseDriver function 'write_path' to write the final output to the desired location
        BaseDriver.write_file(df_agg_veh_make_id, write_path, write_format)
