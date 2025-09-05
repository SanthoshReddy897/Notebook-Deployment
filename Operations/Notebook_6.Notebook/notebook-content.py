# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b96ab740-20f7-478c-b238-c6008e5fdc3b",
# META       "default_lakehouse_name": "Silver",
# META       "default_lakehouse_workspace_id": "2a740b96-2ff1-4755-a85f-64bd7b1803a5",
# META       "known_lakehouses": [
# META         {
# META           "id": "b96ab740-20f7-478c-b238-c6008e5fdc3b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Silver.location LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Silver.users LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
