from pyspark.sql.functions import *

#	Load data set
vehicles = spark.read.option("header",True).option("inferSchema",True).csv('<TEST_DATA>/Electric_Vehicle_Title_and_Registration_Activity.csv.gz')

#   Alias columns to have legal column names
vehiclesClean=vehicles.select(
    col(vehicles.columns[0]).alias('VehicleType'),
    col(vehicles.columns[1]).alias('Vin'),
    col(vehicles.columns[2]).alias('ModelYear'),
    col(vehicles.columns[3]).alias('Make'),
    col(vehicles.columns[4]).alias('Model'),
    col(vehicles.columns[5]).alias('NewUsed'))


#	Persist the range as a delta table
vehiclesClean.write.format("delta").save("<ROOT>/delta")
