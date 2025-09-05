# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "064a45d9-c2be-4048-84b4-2bc77bb299a4",
# META       "default_lakehouse_name": "Arjun_EMPN1689",
# META       "default_lakehouse_workspace_id": "2a740b96-2ff1-4755-a85f-64bd7b1803a5",
# META       "known_lakehouses": [
# META         {
# META           "id": "064a45d9-c2be-4048-84b4-2bc77bb299a4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Arjun_EMPN1689.sales LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
