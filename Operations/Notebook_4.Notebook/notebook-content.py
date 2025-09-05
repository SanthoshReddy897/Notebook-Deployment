# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7ee41409-d40c-4956-bbcf-3950f90cef8b",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "2a740b96-2ff1-4755-a85f-64bd7b1803a5",
# META       "known_lakehouses": [
# META         {
# META           "id": "7ee41409-d40c-4956-bbcf-3950f90cef8b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze.badges LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze.color_srgb LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
