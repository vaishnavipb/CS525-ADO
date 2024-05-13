/*
CS525 - Advanced Database Organization - Assignment 2 - Buffer-Manager

### Submission folder includes - 
        ├── buffer_mgr_stat.c
        ├── buffer_mgr_stat.h
        ├── buffer_mgr.c
        ├── buffer_mgr.h
        ├── dberror.c
        ├── dberror.h
        ├── dt.h
        ├── storage_mgr.c
        ├── storage_mgr.h
        ├── test_assign2_1.c
        ├── test_assign2_2.c
        ├── test_helper.h
        ├── makefile
        └── README.md

### Compilation
The program is equipped with a Makefile that enables compilation. You can initiate the compilation process using the following 
        command: `make` 

### EXECUTION: 
Note: To execute the binary on Linux, make sure to install `gcc` and `make`
        command1: `./test_assign2.1`
        command2: `./test_assign2.2`
For instance,To run LRU alone, comment out testFIFO test case in test_assign2.1.

### Verify Memory Leaks
Note: To verify memory leaks on linux, install `Valgrind`.
Valgrind will provide information about memory leaks and other memory-related issues during the program's execution.
        command1: `valgrind --leak-check=full ./test_assign2.1`
        command2: `valgrind --leak-check=full ./test_assign2.2`
        


### Internal code implementation
This project implements a simple buffer manager for managing fixed number of pages in memory. 

Solution Overview
-----------------
The key capabilities provided by the buffer manager include:
- Managing a fixed-size buffer pool, used to cache disk pages in memory. 
- The buffer manager allows clients to request pages from a page file. 
- The buffer manager checks whether the requested page is already cached and returns a pointer to the page frame if available.
- If the requested page is not cached, the buffer manager reads the page from disk and determines which page frame to store it in based on the chosen replacement strategy.
- Pin pages in memory, read and modify pages.
- Unpin pages, and handle page replacement strategies.

There are two impotant data structures.

- BM BufferPool, which stores information about a buffer pool like the file name, total number of frames, replacement strategy and a pointer to store any necessary information.
- BM PageHandle, which stores istores information about a page like the page number and the pointer to the area in memory storing data

Important Notes
----------------
- The buffer manager should maintain a mapping between page numbers and page frames for efficient look-ups.
- Thread safety is not implemented in this version, and the buffer manager assumes concurrent usage by multiple components of a DBMS.

Code Structure
--------------
The code is divided into the following sections:
- Buffer Pool - methods to create a buffer pool for an existing page file,shutdown a buffer pool and free up all associated resources, and to force the buffer manager to write all dirty pages to disk
- Page Management - methods to pin pages, unpin pages, mark pages as dirty, and force a page back to disk.
- Statistics Functions - methods to return statistics about a buffer pool and its contents

The key functions are
---------------------
- initBufferPool() to create a new buffer pool using page replacememt strategy
- shutdownBufferPool() and forceFlushPool() method to free up resources and write dirty pages to disk
- pinPage() and unpinPage() methods to pin or unpin the specified page
- markDirty() to mark a page dirty, forcePage() to write dity page content to disk
- statistics functions such as getFrameContents(), getDirtyFlags(),getFixCounts(),getNumReadIO() and getNumWriteIO() for the statistical information about buffer pool 
