# Dynamic Parameterization

# To make this notebook dynamic, add widgets:



# Widgets for dynamic input
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("table_location", "")
dbutils.widgets.text("format_type", "delta")
dbutils.widgets.text("mode", "overwrite")

# Fetch widget values
table_name = dbutils.widgets.get("table_name")
table_location = dbutils.widgets.get("table_location")
format_type = dbutils.widgets.get("format_type")
mode = dbutils.widgets.get("mode")

# Example schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Create table
create_table(table_name, schema, table_location, format_type, mode)



# Now, whenever you want to create a new table:

#     Use the widgets to specify table options.
#     Run the notebook with the desired inputs.