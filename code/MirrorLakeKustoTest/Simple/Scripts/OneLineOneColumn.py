#	Create a range of 1 row with one column
data = spark.range(0,1)

#	Persist the range as a delta table
data.write.format("delta").save("<ROOT>/delta")
