from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
	spark = SparkSession.builder.appName("weather_processing").getOrCreate()
	df = spark.read.load("weather_data/2008weather.csv", format="csv", inferSchema="true", header="true")

	df = df.withColumn("Day", F.split(df["1"], ",").getItem(1))\
		.withColumn("DayOfWeek", F.split(df["1"], ",").getItem(0))
	
	df = df.withColumn("Month", F.split(df["Day"], "\s+").getItem(1))\
		.withColumn("DayOfMonth", F.split(df["Day"], "\s+").getItem(2))\
		.drop("1", "Day", "DayOfWeek")
	
	df = df.withColumnRenamed("0", "Year")\
		.withColumnRenamed("2", "AirportCode")\
		.withColumnRenamed("3", "Weather")
	
	df = df.replace(["Jan"], ["1"], "Month")\
		.replace(["Feb"], ["2"], "Month")\
		.replace(["Mar"], ["3"], "Month")\
		.replace(["Apr"], ["4"], "Month")\
		.replace(["May"], ["5"], "Month")\
		.replace(["Jun"], ["6"], "Month")\
		.replace(["Jul"], ["7"], "Month")\
		.replace(["Aug"], ["8"], "Month")\
		.replace(["Sep"], ["9"], "Month")\
		.replace(["Oct"], ["10"], "Month")\
		.replace(["Nov"], ["11"], "Month")\
		.replace(["Dec"], ["12"], "Month")
	
	df = df.replace(["Mon"], ["1"], "DayOfWeek")\
		.replace(["Tue"], ["2"], "DayOfWeek")\
		.replace(["Wed"], ["3"], "DayOfWeek")\
		.replace(["Thu"], ["4"], "DayOfWeek")\
		.replace(["Fri"], ["5"], "DayOfWeek")\
		.replace(["Sat"], ["6"], "DayOfWeek")\
		.replace(["Sun"], ["7"], "DayOfWeek")
	
	df.write.csv("hdfs:///user/maria_dev/weather_data/processing")
