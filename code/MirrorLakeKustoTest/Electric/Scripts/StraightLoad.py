#	Load data set
vehicles = spark.read.option("header",True).option("inferSchema",True).csv('<TEST_DATA>/Electric_Vehicle_Title_and_Registration_Activity.csv.gz')

#	Persist the range as a delta table
data.write.format("delta").save("<ROOT>/delta")
