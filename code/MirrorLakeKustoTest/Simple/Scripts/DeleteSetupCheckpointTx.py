from delta.tables import *
from pyspark.sql.functions import *

#	Read existing table
data = DeltaTable.forPath(spark, "<ROOT>/delta")

# Delete one row
data.delete(condition = expr("id=0"))
