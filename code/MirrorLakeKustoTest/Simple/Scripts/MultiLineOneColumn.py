#	Create a range of 10 rows with one column
data = spark.range(0,10)

#	Persist the range as a delta table
data.write.format("delta").save("<ROOT>/delta")
