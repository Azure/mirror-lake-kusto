#	Create a range of 1 row with one column
data = spark.range(0,1)

#	Persist the range as a delta table
data.write.format("delta").save("<ROOT>/delta")

#   Create ten more transactions
for i in range(1,11):
    data = spark.range(i,i+1)
    data.write.format("delta").mode("append").save("<ROOT>/delta")