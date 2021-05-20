# Cognitive case study

Because it is simpler processing, I ran the job locally, to perform the test, just use the command in a python environment with pyspark:
``
spark-submit spark_job.py
``
Below are some reasons for converting to parquet:

- Reduces IO operations.
- Search for specific columns you need to access.
- It consumes less space.
- Support for type-specific encodings.

# Requirements
1. Conversion of the file format: Convert the CSV file present in the data / input / users / load.csv directory, to a columnar format of high reading performance of your choice. Briefly justify the choice of format;

2. Deduplication of the converted data: In the converted data set there will be multiple entries for the same record, varying only the values ​​of some of the fields between them. It will be necessary to carry out a deduplication process of this data, in order to only keep the last entry of each record, using as reference the id to identify duplicate records and the update date (update_date) to define the most recent record;

3. Conversion of the type of deduplicated data: In the config directory there will be a JSON configuration file (types_mapping.json), containing the names of the fields and the respective desired types of output. Using this file as input, perform a conversion process of the types of the described fields, in the deduplicated data set;

# General notes
- All operations must be performed using Spark. The execution service is at your discretion, being able to use both local services and cloud services. Briefly justify the chosen service (EMR, Glue, Zeppelin, etc.).

- Each operation must be performed on the dataframe resulting from the previous step, and can be persisted and loaded in different sets of files after each step or executed in memory and only persisted after the final operation.

- You are free to follow the desired execution sequence;

- We request the transformation of data types of only some fields. Others are at your discretion

- The final file or set of files must be compressed and sent by email.
