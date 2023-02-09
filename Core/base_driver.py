from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession

class BaseDriver:
    """
    Base_Driver class to create Spark Session. To read and write files.
    """

    def __init__(self, conf):
        """
        Constructor to create the SparkSession and assign raw_file dictionary with null

        Parameters:
        conf (Spark configuration object): Used to create the SparkSession with configuration object
        """

        self.__spark = SparkSession.builder.config(conf=conf).getOrCreate() 
        self.__raw_file = {}

    def read_file(self, name: str, path: str, format: str):
        """
        Function to read a file and save the dataframe in the dictionary as a value

        Parameters:
        name (str): Name of the key in the __raw_file dictionary
        path (str): Location of the dataset
        format (str): Dataset format
        """

        self.__raw_file[name] = self.__spark.read.format(format).option("header", True).load(path)

    def get_raw_file(self, name: str):
        """
        Function to return the dataset based on the key given in parameter

        Parameter:
        name (str): Name of the key in the __raw_file dictionary 

        Return:
        Dataframe or None
        """
        if name in self.__raw_file:
            return self.__raw_file[name]

        return None

    def write_file(dataframe, path: str, format: str):
        """
        Function to write the dataframe in the desired path

        Parameter:
        dataframe (PySpark DataFrame Or string): dataframe or string to be written
        path (str): Location where we need to write the data
        format (str): Format in which the file will be saved
                        If csv then the dataframe will be saved as csv file
                        If txt then the string will be saved in a txt file
        """

        #If format is csv then the dataframe is saved as csv
        if format.lower() == "csv":
            dataframe.write.format("csv").option('header','true').mode("overwrite").save(path)

        #else if the format is txt then the string will be written in the text file
        elif format.lower() == "txt":
            f = open(path+"."+format, "w")
            f.write(dataframe)
            f.close()