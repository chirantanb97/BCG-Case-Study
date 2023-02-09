
## To execute the python files:

Step 1: Install the requirements.txt file using:
        pip install -r requirements.txt

Step 2: Install Apache PySpark, download link: https://spark.apache.org/downloads.html

Step 3: In the Config folder, change the dataset read path under the column name 'Files_Read_Path' 
       in db_config_paths.csv

Step 4: In the Config folder, change the dataset write paths under the column name 'File_Write_Path'
       in db_config_arguments.csv

Step 5: In main.py, change the config_path value to absolute path of Config folder (line no. 41)

Step 6: Execute main.py with arguments (optional). 
       Sample command: spark-submit --master local[1] main.py

----------------------------------------------------------------------------------------------------------------

## Arguments (optional) that can be passed to run solution for a particular problem (or multiple problems):

analytics1 - To run only Analytic 1 problem

analytics2 - To run only Analytic 2 problem

analytics3 - To run only Analytic 3 problem

analytics4 - To run only Analytic 4 problem

analytics5 - To run only Analytic 5 problem

analytics6 - To run only Analytic 6 problem

analytics7 - To run only Analytic 7 problem

analytics8 - To run only Analytic 8 problem

Sample command: spark-submit --master local[1] main.py analytics1 analytics2