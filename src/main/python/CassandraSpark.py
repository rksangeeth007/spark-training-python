from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#Run below command in terminal before running this app:
#cassandra start
#cqlsh
#CREATE KEYSPACE movielens WITH replication = {'class': 'SimpleStrategy', 'replication_factor':'1'} AND durable_writes = true;


#Below command to run this app via spark-submit. This might not run via IJ
#spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 --master local --deploy-mode client /Users/s0h0902/BigDatafinal/Repos/Ultimate_BD_Udemy/cassandra/CassandraSpark.py

def parseInput(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()

    # Get the raw data
    #lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")
    lines = spark.sparkContext.textFile("/Users/s0h0902/BigDataFinal/Repos/Ultimate_BD_Udemy/Datasets/ml-100k-2/u.user")
    # Convert it to a RDD of Row objects with (userID, age, gender, occupation, zip)
    users = lines.map(parseInput)
    # Convert that to a DataFrame
    usersDataset = spark.createDataFrame(users)

    # Write it into Cassandra
    usersDataset.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="users", keyspace="movielens")\
        .save()

    # Read it back from Cassandra into a new Dataframe
    readUsers = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="users", keyspace="movielens")\
    .load()

    readUsers.createOrReplaceTempView("users")

    sqlDF = spark.sql("SELECT * FROM users WHERE age < 20")
    sqlDF.show()


    # Stop the session
    spark.stop()
