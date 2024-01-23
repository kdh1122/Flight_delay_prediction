from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id 

if __name__ == "__main__":
		spark = SparkSession.builder.appName("processing").getOrCreate()
		df = spark.read.load("data/plane.csv",format="csv", sep=",", inferSchema="true", header="true")

		# 결항이나 우회되지 않은 데이터만 filter 
		df = df.filter(df["Cancelled"]==0)
		df = df.filter(df["Diverted"]==0)
		
		# 종류별 지연 컬럼에 있는 결측치 0으로 대체
		df = df.replace(["NA"], ["0"], "CarrierDelay")
		df = df.replace(["NA"], ["0"], "WeatherDelay")
		df = df.replace(["NA"], ["0"], "NASDelay")
		df = df.replace(["NA"], ["0"], "SecurityDelay")
		df = df.replace(["NA"], ["0"], "LateAircraftDelay")
		
		df = df.withColumn("ElapsedTime", df.ArrTime - df.DepTime) # 총 비행 경과 시간 컬럼 추가(도착 시간 - 출발 시간)
		df = df.withColumn("ElapsedTimeDelay", df.CRSElapsedTime - df.ActualElapsedTime) # 지연된 비행 경과 시간 컬럼 추가(예상 비행 경과 시간 - 실제 비행 경과 시간)

		df = df.drop("UniqueCarrier", "FlightNum", "Distance", "AirTime", "TaxiIn", "TaxiOut", "CancellationCode") # 사용하지 않는 컬럼 삭제
		
		# 비행기 운항이 가장 많았던 상위 10개의 공항 추출 
		# 출발 및 도착 공항 별 'Year'컬럼의 행의 수를 count해서 공항 별 운항 수 파악
		airport_dep = df.groupby("Origin").agg(F.count("Year").alias("origin_cnt")).sort(F.desc("origin_cnt")).limit(10)
		airport_arr = df.groupby("Dest").agg(F.count("Year").alias("dest_cnt")).sort(F.desc("dest_cnt")).limit(10)
		
		# 상위 10개 공항의 공항 코드만 추출해서 출발지와 도착지가 상위 10개에 포함되는 데이터만 filter
		airport_list = [row.Origin for row in airport_dep.collect()]
		df = df.filter(df['Origin'].isin(airport_list) & df['Dest'].isin(airport_list))
	
		df.show()
		
