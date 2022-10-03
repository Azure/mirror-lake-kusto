from delta.tables import *

vehicleDelta = DeltaTable.forPath(spark, "<ROOT>/delta")

# Optimize the blobs
vehicleDelta.optimize()
