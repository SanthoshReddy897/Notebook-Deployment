# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f34ae1d0-5973-430e-8547-b0f182b72722",
# META       "default_lakehouse_name": "Gold",
# META       "default_lakehouse_workspace_id": "2a740b96-2ff1-4755-a85f-64bd7b1803a5",
# META       "known_lakehouses": [
# META         {
# META           "id": "f34ae1d0-5973-430e-8547-b0f182b72722"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold.badges LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold.comments LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
