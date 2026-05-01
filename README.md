# CECS-327-Assignment-8

## Pre req installs
* python
* psycopg-2 (for SQL connecting)

## Order to run code
Start the server then the client
In server, enter s to start server t to do test
* python server8.py
* python client8.py

## Connecting to database
Use psycopg-2 library to connect to both partners' databases. We collect the data with SQL queries, first checking databas 1 and getting all house a data stored their (its original dataniz link). House b also checks for all post sharing data in database 1(post sharing). However whenever data is needed from pre sharing house b, access database 2 and get the data. 

## Query Completeness
We confirm the query is complete by using the amount of time needed (past hour week month) against the data avaliable in the first database. If some time does not have data, go check databse 2 to make sure we have full data from the entire sensor generation

## Dataniz Metadata usage
We used the metadata names of the boards and sensors to keep track of what house each sensor's data was from. We created global variables that have all the correct names of each sensor and board correlating to each device in each house. These are what we use to make sure we are getting the right data to calculate with
