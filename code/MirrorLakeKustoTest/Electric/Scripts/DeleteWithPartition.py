from delta.tables import *
from pyspark.sql.functions import *

vehicleDelta = DeltaTable.forPath(spark, "<ROOT>/delta")

# Delete every even value
vehicleDelta.delete(condition = expr("Vin == '1N4AZ0CP6E' AND ModelYear=2014"))
