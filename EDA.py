# 일별 평균 지연 시간
day = df.groupBy('DayofMonth')
        .agg(avg("Arrdelay").alias("avg_arrdelay"), \
        avg("Depdelay").alias("avg_depdelay"))

# 월별 평균 지연 시간
month = df.groupBy("Month")
          .agg(avg("Arrdelay").alias("avg_arrdelay"), \
          avg("Depdelay").alias("avg_depdelay"))
        
# 요일별 평균 지연 시간
dow = df.groupBy("DayOfWeek")
        .agg(avg("Arrdelay").alias("avg_arrdelay"), \
        avg("Depdelay").alias("avg_depdelay"))

# 항공사별 지연 시간
carrier = df.groupby(['UniqueCarrier'])
            .agg(mean('ArrDelay').alias('arrdelay_avg'), \
            mean('DepDelay').alias('depdelay_avg'))

# 출발지 공항별 평균 지연 시간
origin = df.groupBy("Origin")
           .agg(avg("Arrdelay").alias("avg_arrdelay"), \
           max("Arrdelay").alias("max_arrdelay"), \
           min("Arrdelay").alias("min_arrdelay"))

# 도착지 공항별 평균 지연 시간
dest = df.groupBy("Dest")
          .agg(avg("Depdelay").alias("avg_depdelay"), \
          max("Depdelay").alias("max_depdelay"), \
          min("Depdelay").alias("min_depdelay"))

# 날씨 별 지연 빈도
df_weather = df_weather.select("Year", "Month", "DayOfMonth", "AirportCode", split(df_weather["Weather"],",")
                      .alias("WeatherArray"))
df_weather = df_weather.select("Year", "Month", "DayOfMonth", "AirportCode", explode("WeatherArray")
                      .alias("Weather"))
df = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) 
    & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"]))
    .filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"]) 
    | (df_airline["Dest"]==df_weather["AirportCode"]))
df = df.groupby("Weather").agg(count("WeatherDelay").alias("Count"))
       .orderBy(desc("Count"))

# 지연 종류 별 평균 지연 시간
is_delay = df.select(mean("CarrierDelay"), mean("WeatherDelay"), \
                     mean("NASDelay"), mean("SecurityDelay"), mean("LateAircraftDelay"))
