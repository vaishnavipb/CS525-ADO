/*
CS525 - Advanced Database Organization - Assignment 3 - Record Manager

### Submission folder includes - 
        ├── dberror.c
        ├── dberror.h
        ├── storage_mgr.c
        ├── storage_mgr.h
        ├── buffer_mgr.c
        ├── buffer_mgr.h
        ├── buffer_mgr_stat.c
        ├── buffer_mgr_stat.h
        ├── expr.c
        ├── expr.h
        ├── dt.h
        ├── record_mgr.c
        ├── record_mgr.h
        ├── rm_serializer.c
        ├── rm_deserializer.c
        ├── test_expr.c
        ├── tables.h
        ├── test_assign3_1.c
        ├── test_helper.h
        ├── makefile
        └── README.md

### Compilation
The program is equipped with a Makefile that enables compilation. You can initiate the compilation process using the following 
        command: `make` 

### EXECUTION: 
Note: To execute the binary on Linux, make sure to install `gcc` and `make`
        command: `./test_assign3.1`

### Verify Memory Leaks
Note: To verify memory leaks on linux, install `Valgrind`.
Valgrind will provide information about memory leaks and other memory-related issues during the program's execution.
        command: `valgrind --leak-check=full ./test_assign3.1`


### Internal code implementation
This project implements a basic record manager for handling tables with a fixed schema

Solution Overview
-----------------
The key capabilities provided by the record manager include:
- Creating, opening, closing, and deleting tables
- Creating new schemas based on the information provided
- Inserting, updating, deleting and fetching records in a schema
- Performing scans to find the records matched based on the coditions given 
- Accessing and setting values to a record in schema

The solution focuses on managing tables with fixed schemas, also supports CRUD operation and scans. Additionally there is implementation for an optional extension tombstone ID.

Code Structure
--------------
The code is divided into the following sections:
- Tables Management - methods to intialize record manager, shutdown record manager,create, open, close, and delete tables , get num tuples
- Handling Records - methods to insert, delete, update, and get records
- Scans - methods to startScan, move to next and close scan
- Dealing with Schemas - Methods for getting recordsize, creating and free schema
- Record and attribute values - Methods to create and free record, get and set attributes

The key functions are
---------------------
- initRecordManager() and shutdownRecordManager() are used for initialization and shutdown record manager
- createTable(), openTable(), closeTable() and deleteTable() are used for table management operations
- getNumTuples() is used to get the count of the number of records
- startScan(), next(), closeScan() are used to scan the records to find the matches
- readBlock() and writeBlock() core methods to read and write pages
- getRecordSize() is used to get the size of the record in a schema
- createSchema() and freeSchema() are used to create and free the schemas
- createRecord() and freeRecord() are used to create and free records
- getAttr() is used to get the value of an attribute in a record
- setAttr() is used to set the value of an attribute in a record
