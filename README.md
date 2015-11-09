# MyDB
A MySQL like database for parsing huge amounts of data.

Every database operation/query can be implemented using a combination of multiple operations.
These operations when performed in a logical order can be used to fetch data in a very fast manner.

This project takes the files of data to be parsed as input,
generates schema based on select statements,
parses query to read input file line by line
wraps each line of read data around Operator class wrapper,
filters it,
and displays the output of the Query

This code consists of following two main components - 

- Operator Classes: This class emulates the operation performed by each QUERY operation such as SELECT, GROUP BY, etc.
- SELECT MANAGER class: This class parses the query to determine the logical order in which these operators must be applied to the incoming data.

KEY POINTS:
In order to keep minimum data in memory,
- Each row of data is passed through a pipeline.
- One row of data is read at a time and is passed through multiple levels of operations.
- If the data successfully passes through all the operation in pipeline, it is printed.
- An exmaple can be when SELECT operator lets 10 rows of data pass through the pipeline, it reduces to 5 when it passes through WHERE operator, ie, selecting only those rows which pass the WHERE condition.
- Only OPERATOR that fetch the entire data at a time is GROUP BY operator.
