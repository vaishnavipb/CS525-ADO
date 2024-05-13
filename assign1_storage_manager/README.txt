/*
CS525 - Advanced Database Organization - Assignment 1 - Storage-Manager

### Submission folder includes - 
        ├── dberror.c
        ├── dberror.h
        ├── storage_mgr.c
        ├── storage_mgr.h
        ├── test_assign1_1.c
        ├── test_helper.h
        ├── makefile
        └── README.md

### Compilation
The program is equipped with a Makefile that enables compilation. You can initiate the compilation process using the following 
        command: `make` 

### EXECUTION: 
Note: To execute the binary on Linux, make sure to install `gcc` and `make`
        command: `./test_assign1`

### Verify Memory Leaks
Note: To verify memory leaks on linux, install `Valgrind`.
Valgrind will provide information about memory leaks and other memory-related issues during the program's execution.
        command: `valgrind --leak-check=full ./test_assign1`


### Internal code implementation
This project implements a basic storage manager for handling page files.

Solution Overview
-----------------
The key capabilities provided by the storage manager include:
- Creating, opening, closing, and deleting page files
- Reading pages from file into memory
- Writing pages from memory to file
- Tracking current page position
- Appending new pages and ensuring capacity

The core data structure is the SM_FileHandle, which contains metadata about an open page file like the filename, total number of pages, and current page position.

Pages are read from disk into an SM_PageHandle, which represents a page in memory.

The solution focuses on manipulating files at the page level. Pages are treated as fixed sized blocks that can be read, written, and appended.

Code Structure
--------------
The code is divided into the following sections:
- File manipulation - methods to create, open, close, and delete files
- Reading pages - methods to read first, last, next, prev pages
- Writing pages - methods to write and append pages
- Helper methods - for getting current page position, ensuring capacity

The key functions are
---------------------
- openPageFile() and closePageFile() to setup file handle
- readBlock() and writeBlock() core methods to read and write pages
- Auxiliary methods like readFirstBlock(), readPreviousBlock() etc to support reading pages
- appendEmptyBlock() and ensureCapacity() for writing pages
