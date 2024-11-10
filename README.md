# CSVsToSQLServer
This project takes all the xlsx files in a specified directory and inserts the rows of all the files into a table in SQLServer.
Assumes the xlsx files have all the same column names. 
If the process is stopped in the middle of a file, the file will be resumed from the beginning the next time the process is started. 
If a row has already been inserted to SQLServer, it will be skipped the second time. 
Uses a work queue in a SQLite table that tracks which xlsx files have been processed. 

