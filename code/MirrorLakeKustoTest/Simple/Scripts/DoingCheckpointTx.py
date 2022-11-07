#   Create ten more transactions
for i in range(1,11):
    data = spark.range(i,i+1)
    data.write.format("delta").mode("append").save("<ROOT>/delta")