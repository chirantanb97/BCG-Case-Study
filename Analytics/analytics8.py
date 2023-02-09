from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from Core.base_driver import BaseDriver

"""
Analytics 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
             has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states
             with highest number of offences (to be deduced from the data)
"""

class Analytics8(BaseDriver):
    """
    This is a class for Analytics 8 problem which inherits BaseDriver Class
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
        df_charges = self.get_raw_file(name[1])
        df_primary_person = self.get_raw_file(name[2])

        #Checking if the files exist, if not raise Exception
        if df_charges is None:
            raise Exception(f"no input file of name {name[1]} found")
            
        if df_units is None:
            raise Exception(f"no input file of name {name[0]} found")
        
        if df_primary_person is None:
            raise Exception(f"no input file of name {name[2]} found")
        
        #Creating a Window on descending order of count
        windowSpec = Window.orderBy(desc("count"))

        ##Creating a dataframe where drivers are charged with speeding and are having their license 
        #Left join Person_Primary and Charges dataset on CRASH_ID
        df_person_charges = df_primary_person.join(df_charges,["CRASH_ID"],"left")

        #Filter data where accidents are occured due to drivers 
        #and driver license is present(including commercial driver license)
        df_drivers_charges = df_person_charges.filter(lower(col("PRSN_TYPE_ID")).like("%driver%"))\
                                               .filter((lower(col("DRVR_LIC_TYPE_ID")).contains("license"))\
                                                | (lower(col("DRVR_LIC_TYPE_ID")).contains("commercial driver lic.")))

        df_speeding_charges = df_drivers_charges.filter(lower(col("CHARGE")).like("%speed%"))
        df_speeding_charges = df_speeding_charges.select("CRASH_ID").distinct()

        #Joining Units, Person_Primary and Charges dataset on CRASH_ID to get highest charges
        df_unit_person_charges=df_units.join(df_person_charges,["CRASH_ID"],"left")
        
        ##Creating a dataframe with top 10 colours in accidents with highest number of charges
        #Filter out data where colours are 'NA' and count the records based on VEH_COLOR_ID
        df_colour=df_unit_person_charges.filter(col("VEH_COLOR_ID")!="NA").groupBy("VEH_COLOR_ID").count()
        df_ranked_units_colours = df_colour.withColumn("rank",dense_rank().over(windowSpec))
        #Filtering on rank less than equal to 10
        df_top_ten_colours = df_ranked_units_colours.filter(col("rank") <= 10)

        ##Creating a dataframe with top 25 states with highest number of charges
        #Grouping on state and ranking it based on descending order of count
        df_states=df_unit_person_charges.groupBy("DRVR_LIC_STATE_ID").count()
        df_states = df_states.withColumn("rank",dense_rank().over(windowSpec))
        #Filtering on rank less than equal to 25
        df_top_states = df_states.filter(col("rank") <= 25)

        #Applying inner join with Units and the three dataframes that were created 
        df_units_speeding_charges = df_unit_person_charges.join(df_speeding_charges,["CRASH_ID"],"inner")
        df_units_speeding_colour = df_units_speeding_charges.join(df_top_ten_colours,["VEH_COLOR_ID"],"inner")
        df_speeding_colour_state_charges = df_units_speeding_colour.join(df_top_states,["DRVR_LIC_STATE_ID"],"inner")

        #Grouping the final dataframe on 'VEH_MAKE_ID' and ranking it based on desceding order of count
        df_units_make = df_speeding_colour_state_charges.groupBy("VEH_MAKE_ID").count()
        df_top_five_units_make = df_units_make.withColumn("rank",dense_rank().over(windowSpec))
        #Filtering only top 5 'VEH_MAKE_ID'
        df_top_five_units_make = df_top_five_units_make.filter(col("rank") <= 5)

        #Calling BaseDriver function 'write_path' to write the final output to the desired location
        BaseDriver.write_file(df_top_five_units_make, write_path, write_format)