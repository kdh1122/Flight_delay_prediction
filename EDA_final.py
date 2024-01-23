%spark2.pyspark

from pyspark.sql.functions import *
from pyspark.sql.functions import date_format
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

df_2008 = spark.read.csv("hdfs:///user/maria_dev/pro/2008resultfull.csv", header=True, inferSchema=True)
df_2007 = spark.read.csv("hdfs:///user/maria_dev/pro/2007resultfull.csv", header=True, inferSchema=True)
df_2006 = spark.read.csv("hdfs:///user/maria_dev/pro/2006resultfull.csv", header=True, inferSchema=True)
df_2005 = spark.read.csv("hdfs:///user/maria_dev/pro/2005resultfull.csv", header=True, inferSchema=True)
df_2004 = spark.read.csv("hdfs:///user/maria_dev/pro/2004resultfull.csv", header=True, inferSchema=True)
df_2003 = spark.read.csv("hdfs:///user/maria_dev/pro/2003resultfull.csv", header=True, inferSchema=True)

df_airline_u = df_2003.union(df_2004).union(df_2005).union(df_2006).union(df_2007).union(df_2008)
df_airline=df_airline_u
df_airline = df_airline.filter(df_airline["Cancelled"]==0).filter(df_airline["Diverted"]==0)
df_airline = df_airline.replace(["NA"], ["0"], "CarrierDelay").replace(["NA"], ["0"], "WeatherDelay")
df_airline = df_airline.replace(["NA"], ["0"], "NASDelay").replace(["NA"], ["0"], "SecurityDelay").replace(["NA"], ["0"], "LateAircraftDelay")
df_airline = df_airline.withColumn("ElapsedTime", df_airline.ArrTime - df_airline.DepTime)
df_airline = df_airline.withColumn("CRSElapsedTime", df_airline.CRSArrTime - df_airline.CRSDepTime)
df_airline = df_airline.withColumn("ElapsedTimeDelay", df_airline.CRSElapsedTime - df_airline.ActualElapsedTime)
df_airline = df_airline.drop("DayOfWeek", "FlightNum", "Distance", "AirTime", "TaxiIn", "TaxiOut", "CancellationCode")
df_airline_new = df_airline

##///노선별 실제와 예상 비행시간 차이
df = df_airline_new
result_df = df.groupBy("Origin","Dest").agg(F.mean("ElapsedTimeDelay").alias("ETDM"))
z.show(result_df)

##//운항 노선 갯수,딜레이 출발지별
result_df = df.groupBy("Origin").agg(F.count("ElapsedTimeDelay").alias("ETDC")).sort("ETDC")
result_df1 = df.groupBy("Origin").agg(F.mean("ElapsedTimeDelay").alias("ETDM")).sort("ETDM")

result_df2 = result_df.join(result_df1,result_df1["Origin"]==result_df["Origin"])

result_df2 = result_df2.sort("ETDC")

z.show(result_df2)
##//운항 노선 갯수,딜레이 도착지별
result_df = df.groupBy("Dest").agg(F.count("ElapsedTimeDelay").alias("ETDC")).sort("ETDC")
result_df1 = df.groupBy("Dest").agg(F.mean("ElapsedTimeDelay").alias("ETDM")).sort("ETDM")

result_df2 = result_df.join(result_df1,result_df1["Dest"]==result_df["Dest"])

result_df2 = result_df2.sort("ETDC")

z.show(result_df2)


##출발 시간대별 시간 딜레이 정도

airport_dep = df_airline.groupby("Origin").agg(count("Year").alias("origin_cnt")).sort(desc("origin_cnt")).limit(10)
airport_arr = df_airline.groupby("Dest").agg(count("Year").alias("dest_cnt")).sort(desc("dest_cnt")).limit(10)
airport_list = [row.Origin for row in airport_dep.collect()]
df_airline = df_airline.filter(df_airline["Origin"].isin(airport_list) & df_airline["Dest"].isin(airport_list))

df_weather = df_weather.select("Year", "Month", "DayOfMonth", "AirportCode", split(df_weather["Weather"],",").alias("WeatherArray"))
df_weather = df_weather.select("Year", "Month", "DayOfMonth", "AirportCode", explode("WeatherArray").alias("Weather"))
df = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"]))

df_1 = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"])).filter(df_airline["CRSDepTime"] < 600)
df_2 = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"])).filter((df_airline["CRSDepTime"] < 1200) & (df["CRSDepTime"] >= 600))
df_3 = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"])).filter((df_airline["CRSDepTime"] < 1800) & (df["CRSDepTime"] >= 1200))
df_4 = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"])).filter((df_airline["CRSDepTime"] < 2400) & (df["CRSDepTime"] >= 1800))


###출발 시간대별 실제 딜레이 시간
df1 = df_1.agg(mean("ElapsedTimeDelay").alias("ETDM"))
df2 = df_2.agg(mean("ElapsedTimeDelay").alias("ETDM"))
df3 = df_3.agg(mean("ElapsedTimeDelay").alias("ETDM"))
df4 = df_4.agg(mean("ElapsedTimeDelay").alias("ETDM"))
df_result= df1.union(df2).union(df3).union(df4)
z.show(df_result)

###출발 시간대별 도착 딜레이 
df1 = df_1.agg(mean("ArrDelay").alias("ArrM"))
df2 = df_2.agg(mean("ArrDelay").alias("ArrM"))
df3 = df_3.agg(mean("ArrDelay").alias("ArrM"))
df4 = df_4.agg(mean("ArrDelay").alias("ArrM"))
df_result= df1.union(df2).union(df3).union(df4)
z.show(df_result)

###출발 시간대별 출발 딜레이.
df1 = df_1.agg(mean("DepDelay").alias("DepM"))
df2 = df_2.agg(mean("DepDelay").alias("DepM"))
df3 = df_3.agg(mean("DepDelay").alias("DepM"))
df4 = df_4.agg(mean("DepDelay").alias("DepM"))
df_result= df1.union(df2).union(df3).union(df4)
z.show(df_result)


##//운항 노선 갯수,딜레이 출발지별
result_df = df.groupBy("Origin").agg(F.count("ElapsedTimeDelay").alias("ETDC")).sort("ETDC")
result_df1 = df.groupBy("Origin").agg(F.mean("ElapsedTimeDelay").alias("ETDM")).sort("ETDM")
result_df2 = result_df.join(result_df1,result_df1["Origin"]==result_df["Origin"])
result_df2 = result_df2.sort("ETDC")

z.show(result_df2)
##//운항 노선 갯수,딜레이 도착지별
result_df = df.groupBy("Dest").agg(F.count("ElapsedTimeDelay").alias("ETDC")).sort("ETDC")
result_df1 = df.groupBy("Dest").agg(F.mean("ElapsedTimeDelay").alias("ETDM")).sort("ETDM")
result_df2 = result_df.join(result_df1,result_df1["Dest"]==result_df["Dest"])
result_df2 = result_df2.sort("ETDC")
z.show(result_df2)

##///노선별 평균 지연 시간
df = df_airline_new
result_df = df.groupBy("Origin","Dest").agg(F.mean("ElapsedTimeDelay").alias("ETDM"))
z.show(result_df)

##//운항 노선 갯수,딜레이 도착지별
result_df = df.groupBy("Dest").agg(F.count("ElapsedTimeDelay").alias("ETDC")).sort("ETDC")
result_df1 = df.groupBy("Dest").agg(F.mean("ElapsedTimeDelay").alias("ETDM")).sort("ETDM")
result_df2 = result_df.join(result_df1,result_df1["Dest"]==result_df["Dest"])
result_df2 = result_df2.sort("ETDC")
z.show(result_df2)



####
# 출발지 날씨 기준 정렬
df_airline = df_airline_new
df = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"]))
result_df = df.groupby("Weather").agg(mean("ElapsedTimeDelay").alias("ETDM"))
result_df1 = df.groupby("Weather").agg(count("Weather").alias("Count"))
result_df2 = result_df.join(result_df1,result_df1["Weather"]==result_df["Weather"])
result_df2 = result_df2.orderBy(desc("ETDM"))
z.show(result_df2)

# 도착 날씨 기준 정렬
df = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Dest"]==df_weather["AirportCode"]))
result_df = df.groupby("Weather").agg(mean("ElapsedTimeDelay").alias("ETDM"))
result_df1 = df.groupby("Weather").agg(count("Weather").alias("Count"))
result_df2 = result_df.join(result_df1,result_df1["Weather"]==result_df["Weather"])
result_df2 = result_df2.orderBy(desc("ETDM"))
z.show(result_df2)




######
#출발 시간대별 딜레이 정도
(df_weather["DayOfMonth"]==df_airline["DayOfMonth"]).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"]))
df1 = df.filter(df["CRSDepTime"] < 600).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df2 = df.filter((df["CRSDepTime"] < 1200) & (df["CRSDepTime"] >= 600)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df3 = df.filter((df["CRSDepTime"] < 1800) & (df["CRSDepTime"] >= 1200)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df4 = df.filter((df["CRSDepTime"] < 2400) & (df["CRSDepTime"] >= 1800)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df_result= df1.union(df2).union(df3).union(df4)



#도착 시간대별 딜레이 정도.
(df_weather["DayOfMonth"]==df_airline["DayOfMonth"]).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"]))
df1 = df.filter(df["CRSDepTime"] < 600).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df2 = df.filter((df["CRSDepTime"] < 1200) & (df["CRSDepTime"] >= 600)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df3 = df.filter((df["CRSDepTime"] < 1800) & (df["CRSDepTime"] >= 1200)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df4 = df.filter((df["CRSDepTime"] < 2400) & (df["CRSDepTime"] >= 1800)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df_result= df1.union(df2).union(df3).union(df4)
z.show(df_result)



df= df_airline_u

df = df.filter(df["Cancelled"]==0)
df = df.filter(df["Diverted"]==0)

df = df.replace(["NA"], ["0"], "CarrierDelay")
df = df.replace(["NA"], ["0"], "WeatherDelay")
df = df.replace(["NA"], ["0"], "NASDelay")
df = df.replace(["NA"], ["0"], "SecurityDelay")
df = df.replace(["NA"], ["0"], "LateAircraftDelay")
											
df = df.withColumn("ElapsedTime", df.ArrTime - df.DepTime)
df = df.withColumn("CRSElapsedTime", df.CRSArrTime - df.CRSDepTime)
df = df.withColumn("ElapsedTimeDelay", df.CRSElapsedTime - df.ActualElapsedTime)

df = df.drop("UniqueCarrier", "FlightNum", "Distance", "AirTime", "TaxiIn", "TaxiOut", "CancellationCode")							
airport_dep = df.groupby("Origin").agg(F.count("Year").alias("origin_cnt")).sort(F.desc("origin_cnt")).limit(10)
airport_arr = df.groupby("Dest").agg(F.count("Year").alias("dest_cnt")).sort(F.desc("dest_cnt")).limit(10)
		
airport_list = [row.Origin for row in airport_dep.collect()]
df = df.filter(df['Origin'].isin(airport_list) & df['Dest'].isin(airport_list))

# 도착 공항 별 평균 도착 지연 시간
is_arrdelay = df.filter("ArrDelay > 0") # 도착 지연이 발생한 행만 filter
count_arrdelay = is_arrdelay.groupby("Dest").agg(F.mean("ArrDelay").alias("delarr")).sort("delarr")
z.show(count_arrdelay)

# 출발 공항 별 평균 출발 지연 시간
is_depdelay = df.filter("DepDelay > 0") # 출발 지연이 발생한 행만 filter
count_depdelay = is_depdelay.groupby("Origin").agg(F.mean("DepDelay").alias("deldep")).sort("deldep")
z.show(count_depdelay)

# 딜레이 별 평균 지연 시간
# 지연이 발생하지 않은 날은 제외하기 위해 0을 결측치로 변환
df = df.replace(["0"], ["NA"], "CarrierDelay")
df = df.replace(["0"], ["NA"], "WeatherDelay")
df = df.replace(["0"], ["NA"], "NASDelay")
df = df.replace(["0"], ["NA"], "SecurityDelay")
df = df.replace(["0"], ["NA"], "LateAircraftDelay")

is_delay = df.select(F.mean("CarrierDelay"), F.mean("WeatherDelay"), F.mean("NASDelay"), F.mean("SecurityDelay"), F.mean("LateAircraftDelay"))
z.show(is_delay)



