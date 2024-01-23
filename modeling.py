from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, column
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import IndexToString

if __name__ == "__main__":
	spark = SparkSession.builder.appName("processing").getOrCreate()
	df = spark.read.load("project/plane_2008.csv",format="csv", sep=",", inferSchema="true", header="true")	
	
	df = df.filter(df["Cancelled"]==0)
	df = df.filter(df["Diverted"]==0)
	df = df.replace(["NA"], ["0"], "CarrierDelay")
	df = df.replace(["NA"], ["0"], "WeatherDelay")
	df = df.replace(["NA"], ["0"], "NASDelay")
	df = df.replace(["NA"], ["0"], "SecurityDelay")
	df = df.replace(["NA"], ["0"], "LateAircraftDelay")
	df = df.withColumn("CarrierDelay", col("CarrierDelay").cast("int"))
	df = df.withColumn("WeatherDelay", col("WeatherDelay").cast("int"))
	df = df.withColumn("NASDelay", col("NASDelay").cast("int"))
	df = df.withColumn("SecurityDelay", col("SecurityDelay").cast("int"))
	df = df.withColumn("LateAircraftDelay", col("LateAircraftDelay").cast("int"))
	df = df.withColumn("ElapsedTime", df.ArrTime - df.DepTime)
	df = df.withColumn("CRSElapsedTime", df.CRSArrTime - df.CRSDepTime)
	df = df.withColumn("ElapsedTimeDelay", df.CRSElapsedTime - df.ActualElapsedTime)
	
	df = df.withColumn("ActualElapsedTime", col("ActualElapsedTime").cast("int"))
        df = df.withColumn("ArrDelay", col("ArrDelay").cast("int"))
	df = df.withColumn("DepDelay", col("DepDelay").cast("int"))
	df = df.withColumn("ArrTime", col("ArrTime").cast("int"))
	
	df = df.drop("TailNum", "FlightNum", "Distance", "AirTime", "TaxiIn", "TaxiOut", "CancellationCode")

	airport_dep = df.groupby("Origin").agg(F.count("Year").alias("origin_cnt")).sort(F.desc("origin_cnt")).limit(10)
	airport_arr = df.groupby("Dest").agg(F.count("Year").alias("dest_cnt")).sort(F.desc("dest_cnt")).limit(10)
	airport_list = [row.Origin for row in airport_dep.collect()]
	df = df.filter(df['Origin'].isin(airport_list) & df['Dest'].isin(airport_list))
	 
	indexer1 = StringIndexer(inputCol="Origin",outputCol="Origin2")
	indexer2 = StringIndexer(inputCol="Dest",outputCol="Dest2")
	indexer3 = StringIndexer(inputCol="UniqueCarrier",outputCol="UniqueCarrier2")
	df1 = indexer1.fit(df).transform(df)
       	df2 = indexer2.fit(df1).transform(df1)
	encoding_df =  indexer3.fit(df2).transform(df2)
	
	df = encoding_df.drop("Origin","Dest","UniqueCarrier")	
	
	feature_list = df.columns
	feature_list.remove("ArrTime")
	feature_list.remove("DepTime")	
	
	vecAssembler = VectorAssembler(inputCols=feature_list, outputCol="features")
	lr = LinearRegression(featuresCol="features", labelCol="Origin2").setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
	trainDF, testDF = df.randomSplit([0.8, 0.2], seed=42)
	
	print(trainDF.cache().count()) # Cache because accessing training data multiple times
	print(testDF.count())

	pipeline = Pipeline(stages=[vecAssembler, lr])
	pipelineModel = pipeline.fit(trainDF)
	predDF = pipelineModel.transform(testDF)
	predAndLabel = predDF.select("prediction","Origin2")
	stringer = IndexToString(inputCol='Origin2', outputCol='Origin_res')
	ecoding_df = stringer.transform(encoding_df)
	predAndLabel.show()	

	evaluator = RegressionEvaluator()
	evaluator.setPredictionCol("prediction")
	evaluator.setLabelCol("Origin")
	print(evaluator.evaluate(predAndLabel, {evaluator.metricName: "r2"}))
	print(evaluator.evaluate(predAndLabel, {evaluator.metricName: "mae"}))
	print(evaluator.evaluate(predAndLabel, {evaluator.metricName: "rmse"}))

