Requirement: Design a datawarehouse to store data about all the songs being played by different users. The idea is to make the data avilable for analysis.

Solution:

1. Create a datawarehouse with 4 dimension tables and a fact table:
    users - user dimension and its attributes
    songs - song dimension and its attributes
    artists - artist dimension and its attributes
    time - time dimension and its attributes
    songplays - fact table with all songs played on the system along with links to the different dimensions
    
    Source data is available in 2 json files in a specific S3 bucket.
    
2. Insert data into the datawarehouse

The solution contain the following files:

1. sql_queries.py: contains all the sql queries required to drop and create the database objects and the insert queries to insert data into the tables.
2. create_tables.py: contains functions to create the fact and dimension tables as well as the stage tables.
3. etl.py: Parse the json files and insert data into the stage tables. Finally use the stage tables and insert data into the fact and dimension tables.
4. dwh.cfg: contains information to configure the cloud Redshift database connection.

Steps:

1. Open a new jupyter notebook file.
2. execute the below statement to create the database objects:
    %run create_tables.py
2. execute the below statment to populate the data warehouse:
    %run etl.py
3. Test the data to make sure all tables have been populated properly.
