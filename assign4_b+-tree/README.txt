/*
CS525 - Advanced Database Organization - Assignment 4 - B+-Tree

Submitted By:
------------- 
Group #39
Members:
        sgutha@hawk.iit.edu (Sona Shree Reddy Gutha, A20532599),
        vpapudesibabu@hawk.iit.edu (Vaishnavi Papudesi Babu, A20527963)
*/ 


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
        ├── btree_mgr.c
        ├── btree_mgr.h
        ├── test_expr.c
        ├── tables.h
        ├── test_assign4_1.c
        ├── test_helper.h
        ├── makefile
        └── README.md

### Compilation
The program is equipped with a Makefile that enables compilation. You can initiate the compilation process using the following 
        command: `make` 

### EXECUTION: 
Note: To execute the binary on Linux, make sure to install `gcc` and `make` follow below example commands 
        To run the binary
        command: `./test_assign4_1`

### Verify Memory Leaks
Note: To verify memory leaks on linux, install `Valgrind`.
Valgrind will provide information about memory leaks and other memory-related issues during the program's execution.
        command: `valgrind --leak-check=full ./test_assign4_1`


### Internal code implementation
This project iimplements a B+-tree index, backed by a page file and managed through buffer manager. Each node occupies a single page, and the datatype supported for keys is integers (DT_INT).

Solution Overview
-----------------
The key capabilities provided by the B+ tree include:
- Leaf Split in leaf nodes during insertion and ensure balanced distribution of keys.
- Non-Leaf Split in internal nodes, ensuring that the middle key moves up to the parent node.
- Handling of node underflow through redistribution or merging of nodes and prioritizing left siblings for redistribution.
- functionality to print the tree structure for debugging.

This solution focuses on creation and deletion on btrees, insertion, search and deletion of the nodes and performs scans.

Code Structure
--------------
The code is divided into the following sections:
- B+-tree Functions - methods to create, open, close or delete the b+ tree index
- Index Manager Functions - methods to to initialize or shutdoum b+ tree index manager
- Key Functions - methods to search, insert, delete key and also to perform scans.
- Debug functions - Methods to print the B+ tree for the testing and debugging purposes

The key functions are
---------------------
- initIndexManager() and shutdownIndexManager() are used for initialization and shutdown B+ tree index manager
- createBtree() initializes a new B+-tree
- openBtree() opens an existing B+-tree
- closeBtree() closes B+-tree
- deleteBtree() deletes a B+-tree
- getNumNodes() is used to get the number of nodes in the B+-tree
- getNumEntries() is used to return the count of entries in the B+-tree
- getKeyType() is used to get the datatype of the key in B+-tree
- findKey() is used to search for the key and returns the record id
- insertKey() is used to insert the key and record ID pair into the B+-tree 
- deleteKey() is used to remove the key from the B+-tree
- openTreeScan() is used to initialize scan operation on B+-tree
- nextEntry() is used to check if there is next entry for the scan and gets the next entry to scan
- closeTreeScan() is used to close the scan
- printTree() is used to print the B+-tree for test and debugging purposes