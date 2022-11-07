#	Read existing table
data = DeltaTable.forPath(spark, "<ROOT>/delta")

# Delete one row
data.delete(condition = expr("id=2020"))
