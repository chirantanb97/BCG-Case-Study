from pyspark.sql.functions import *
from pyspark.sql.types import *
from Core.base_driver import BaseDriver

"""
Analytics 2: How many two wheelers are booked for crashes?
"""

class Analytics2(BaseDriver):
    """
    This is a class for Analytics 2 problem which inherits BaseDriver Class
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

        #Filtering where helmet is 'not applicable'(only two wheel vehicles helmet is applicable)
        df_two_wheeler_accident = df_primary_person.filter(~(lower(col("PRSN_HELMET_ID")) == "not applicable"))
        
        #Counting the number of accidents involving two wheelers(including cyclist)
        df_two_wheeler_accident_count = df_two_wheeler_accident.count()
        #formating the text
        df_two_wheeler_accident_text = "Number of two wheelers that are booked for crashes: {0}".format(df_two_wheeler_accident_count)

        #Calling BaseDriver function 'write_path' to write the final output to the desired location
        BaseDriver.write_file(df_two_wheeler_accident_text, write_path, write_format)