# Generic Notebook for Table Creation
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import datetime

# Function to create a table
def create_table(table_name, schema, table_location, format_type="delta", mode="overwrite"):
    """
    Create a table dynamically based on the provided options.

    Args:
        table_name (str): Name of the table to create.
        schema (StructType): Schema for the table.
        table_location (str): Path to save the table.
        format_type (str): Format of the table (default is "delta").
        mode (str): Save mode (default is "overwrite").
    """
    try:
        print(f"[INFO] Starting table creation: {table_name}")

        # Create an empty DataFrame with the schema
        df = spark.createDataFrame([], schema)

        # Write the DataFrame to the specified location
        df.write.format(format_type).mode(mode).save(table_location)

        # Register the table in the metastore
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING {format_type} LOCATION '{table_location}'")

        print(f"[INFO] Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to create table '{table_name}': {e}")

# Example Usage
if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("TableCreationNotebook").getOrCreate()

    # Table creation parameters (to be replaced with dynamic inputs)
    table_name = "example_table"
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    table_location = "/mnt/example_table"
    format_type = "delta"
    mode = "overwrite"

    # Create the table
    create_table(table_name, schema, table_location, format_type, mode)
