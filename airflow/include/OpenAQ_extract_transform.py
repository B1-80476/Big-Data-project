from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import os
import sys
from datetime import datetime, timedelta

location_parameters = {
    8118: ["pm25"],  # New Delhi
    8172: ['pm25'],  # Kolkata
    8039: ['pm25'],  # Mumbai
    8557: ['pm25'],  # Hyderabad
    8558: ['pm25']  # Chennai
}

# get the execution date of the DAG
date1 = sys.argv[1]
print(f'date1 = {date1}')
date1 = date1[1:11]
date1 = datetime.strptime(date1, '%Y-%m-%d').date()
date2 = date1 + timedelta(days=1)

date1 = str(date1)[:10]
date2 = str(date2)[:10]

# hitting the api and getting the data for the dates
dy = date1[-2:]
mnt = date1[5:7]
yr = date1[:4]

# create a SparkSession
spark = SparkSession.builder.appName('read_local_data').getOrCreate()

# reading the local data
for location_id in location_parameters.keys():
    for parameter in location_parameters[location_id]:
        try:
            if os.path.exists(
                    f'/home/sad7_5407/Desktop/Data_Engineering/data/{yr}/{mnt}/{dy}/{location_id}/{parameter}.json'):
                local_data = spark.read \
                    .option('multiline', True) \
                    .json(
                    f'/home/sad7_5407/Desktop/Data_Engineering/data/{yr}/{mnt}/{dy}/{location_id}/{parameter}.json')
                print(f'>>>>>>> {yr}/{mnt}/{dy}/{location_id}/{parameter}.json read success..', end=' ')

                # selecting the required columns
                final_df = local_data.select('locationId', 'date.utc', 'parameter', 'value')

                # dropping the duplicates if any
                final_df = final_df.drop_duplicates()

                # dumping into warehouse
                final_df.write.format("orc") \
                    .mode('append').save("hdfs://localhost:9000/user/OpenAQ/data/input")
                print(f' dumped to HDFS warehouse ...')

        except AnalysisException:
            print(f'>>>>>>>>> inferSchema failed for data/{yr}/{mnt}/{dy}/{location_id}/{parameter}..')

spark.stop()
