# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d892f777-0cbe-4d34-ac92-a5259e91bcca",
# META       "default_lakehouse_name": "SalesLakehouse_fastrack",
# META       "default_lakehouse_workspace_id": "2a740b96-2ff1-4755-a85f-64bd7b1803a5",
# META       "known_lakehouses": [
# META         {
# META           "id": "d892f777-0cbe-4d34-ac92-a5259e91bcca"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/curated/AMER/15-06-2025/sales_cleaned.csv")
# df now is a Spark DataFrame containing CSV data from "Files/curated/AMER/15-06-2025/sales_cleaned.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.text("Files/curated/EMEA/2025-06-15/processing_summary.txt")
# df now is a Spark DataFrame containing text data from "Files/curated/EMEA/2025-06-15/processing_summary.txt".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
