from delta.tables import *
from pyspark.sql.functions import *

vehicleDelta = DeltaTable.forPath(spark, "<ROOT>/delta")

# Delete every even value
vehicleDelta.delete(condition = expr("ModelYear=2020"))
