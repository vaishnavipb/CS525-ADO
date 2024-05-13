#include <stdlib.h>
#include "dberror.h"

#include "storage_mgr.h"
#include "buffer_mgr.h"

/**
 * Contains information about a buffer manager page frame
 */
long globalTime = 0;

/*Buffer Pool Functions - BEGIN*/

/**
 * Method to initialize buffermanager page frame
 */
static void initBMPageFrame(BM_PageFrame *page, int frameNumber, int numPages)
{
    page->data = (char *)malloc(PAGE_SIZE * sizeof(char));
    page->frameNumber = frameNumber;
    page->pageNumber = -1;
    page->fixCount = 0;
    page->isDirty = false;
    page->timeStamp = 0;
    page->previousFrame = (frameNumber == 0) ? NULL : &page[-1];
    page->nextFrame = (frameNumber == numPages - 1) ? NULL : &page[1];
}

/**
 * Method to creates a new buffer pool with numPages page frames using the page replacement strategy.
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData)
{
    bm->pageFile = (char *)pageFileName;                              // sets page file name to buffer pool
    bm->numPages = numPages;                                          // sets number of frames to the buffer pool
    bm->strategy = strategy;                                          // sets the strategy used to the buffer pool
    BM_PoolInfo *bpInfo = (BM_PoolInfo *)malloc(sizeof(BM_PoolInfo)); // dynamically allocate memory to bufferpoolinfo and returns pointer to allocated memory

    BM_PageFrame *bufferPool = (BM_PageFrame *)malloc(numPages * sizeof(BM_PageFrame)); // dynamically allocate memory to pageframe and returns pointer to allocated memory

    for (int i = 0; i < numPages; i++) // iterates through the number of frames in bufferpool
    {
        initBMPageFrame(&bufferPool[i], i, numPages); // initializes buffer manager page frame
    }

    bpInfo->head = &bufferPool[0];              // setting head of buffer pool info to the address of the first element of the pageframe array
    bpInfo->tail = &bufferPool[numPages - 1];   // setting tail of buffer pool info to the address of the last element of the pageframe array
    bpInfo->begin = &bufferPool[0];             // setting begin of buffer pool info to the address of the first element of the pageframe array
    bpInfo->tail->nextFrame = bpInfo->head;     // sets nextframe member of tail to the value head of buffer pool info
    bpInfo->head->previousFrame = bpInfo->tail; // sets previous frame member of head to the value tail of buffer pool info

    bpInfo->bufferPool = bufferPool;          // sets bufferpool of bufferpoolinfo to the address of bufferpool
    bpInfo->head = &bufferPool[0];            // setting head of buffer pool info to the address of the first element of the pageframe array
    bpInfo->tail = &bufferPool[numPages - 1]; // setting tail of buffer pool info to the address of the last element of the pageframe array
    bpInfo->readNumber = 0;                   // number of pages read from the disk is initialized to zero
    bpInfo->writeNumber = 0;                  // number of pages written to the disk is initialized to zero
    bpInfo->framesCount = 0;                  // frame count is initialized to zero

    bm->mgmtData = bpInfo; // buffer pool info is assigned to mgmt data for bookkeeping
    globalTime = 0;        // initialize global time to 0
    return RC_OK;          // returns successful response
}

/**
 * Method to destroys a buffer pool and frees up all resouces associated with buffer pool
 */
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    RC rc;
    // initializing the pointer variable buffPoolInfo to point to the memory location of mgmtData of bm.
    BM_PoolInfo *bpInfo = bm->mgmtData;
    // Force flush all dirty pages to disk
    rc = forceFlushPool(bm);
    // if the response is not successful return the error code
    if (rc != RC_OK)
    {
        return rc;
    }

    // Clear the data, previousFrame and nextFrame pointers in the page frame list
    for (int i = 0; i < bm->numPages; i++)
    {
        BM_PageFrame *page = &(bpInfo->bufferPool[i]); // initializing page pointer to point to ith address of buffer pool
        free(bpInfo->bufferPool[i].data);              // frees up dynamically allocated memory pointer to by the data of ith element of bufferpool
        page->previousFrame = NULL;                    // sets previous frame to null
        page->nextFrame = NULL;                        // sets next frame to null
    }

    free(bpInfo->bufferPool); // frees up the entire dynamically allocated memory for the bufferpool array
    return RC_OK;             // returns successful response
}
/**
 * Method causes all dirty pages (with fix count 0) from the buffer pool to be written to disk.
 */
RC forceFlushPool(BM_BufferPool *const bm)
{
    SM_FileHandle fHandle;
    RC rc;
    // initializing the pointer variable buffPoolInfo to point to the memory location of mgmtData of bm.
    BM_PoolInfo *bpInfo = bm->mgmtData;
    // Opens an existing page file, else returns error if file not found
    rc = openPageFile(bm->pageFile, &fHandle);
    // if the return code is not successfull, returns the error code
    if (rc != RC_OK)
    {
        return rc;
    }

    for (int i = 0; i < bm->numPages; i++) // iterates through the number of frames in buffer pool
    {
        BM_PageFrame *page = &(bpInfo->bufferPool[i]); // initializing page pointer to point to ith address of buffer pool
        if (page->isDirty && page->fixCount == 0)      // if the page is marked as dirty and the pages are not pinnes i.e fix count is 0
        {
            rc = writeBlock(page->pageNumber, &fHandle, page->data); // write a page to disk at an absolute position
            // if the response is unsuccessful, close the page file and return error code
            if (rc != RC_OK)
            {
                closePageFile(&fHandle);
                return rc;
            }

            page->isDirty = false; // resets isDirty to false

            bpInfo->writeNumber++; // increase the count of the writeNumber of buffer pool info to keep track of write operations to disk
        }
    }

    rc = closePageFile(&fHandle); // closes the file
    if (rc != RC_OK)
    {
        return rc; // returns error code in case of unsuccessful response
    }

    return RC_OK; // returns successful response
}

/*Buffer Pool Functions - END*/

/*Page Management Functions - BEGIN*/

RC FIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    SM_FileHandle fh;
    openPageFile((char *)bm->pageFile, &fh);

    BM_PoolInfo *bpInfo = bm->mgmtData;
    BM_PageFrame *q = bpInfo->head;

    while (q != bpInfo->head)
    {
        if (q->pageNumber == pageNum)
        {
            page->pageNum = pageNum;
            page->data = q->data;

            q->fixCount++;
            // closePageFile(&fh);
            return RC_OK;
        }
        q = q->nextFrame;
    }

    if (bpInfo->framesCount >= bm->numPages)
    {
        q = bpInfo->tail;
        while (q != bpInfo->head)
        {
            if (q->fixCount == 0)
            {
                if (q->isDirty)
                {
                    ensureCapacity(q->pageNumber, &fh);
                    if (writeBlock(q->pageNumber, &fh, q->data) != RC_OK)
                    {
                        // closePageFile(&fh);
                        return RC_WRITE_FAILED;
                    }
                    bpInfo->writeNumber++;
                }

                q->pageNumber = pageNum;
                q->fixCount++;
                bpInfo->tail = q->nextFrame;
                bpInfo->head = q;

                break;
            }
            q = q->nextFrame;
        }
    }
    else
    {
        q = bpInfo->head;
        q->pageNumber = pageNum;

        if (q->nextFrame != bpInfo->head)
        {
            bpInfo->head = q->nextFrame;
        }
        q->fixCount++;
        bpInfo->framesCount++;
    }

    ensureCapacity((pageNum + 1), &fh);
    if (readBlock(pageNum, &fh, q->data) != RC_OK)
    {
        // closePageFile(&fh);
        return RC_OK;
    }
    bpInfo->readNumber++;

    page->pageNum = pageNum;
    page->data = q->data;
    // closePageFile(&fh);

    return RC_OK;
}

BM_PageFrame *findFrameInBufferPool(BM_PoolInfo *bp_mgmt, const PageNumber pageNum);
void updatePageAndFrame(BM_PageHandle *const page, BM_PageFrame *frame, const PageNumber pageNum, BM_PoolInfo *bp_mgmt);
BM_PageFrame *allocateEmptyFrame(BM_PoolInfo *bp_mgmt, const PageNumber pageNum);
BM_PageFrame *replacePage(BM_BufferPool *const bm, BM_PoolInfo *bp_mgmt, SM_FileHandle *fh, const PageNumber pageNum);

RC LRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    BM_PoolInfo *bp_mgmt = bm->mgmtData;
    BM_PageFrame *frame = bp_mgmt->head;
    SM_FileHandle fh;

    openPageFile((char *)bm->pageFile, &fh);

    // Check if the frame is already in the buffer pool
    frame = findFrameInBufferPool(bp_mgmt, pageNum);
    if (frame != NULL)
    {
        updatePageAndFrame(page, frame, pageNum, bp_mgmt);
        return RC_OK;
    }

    // If there are empty spaces in the buffer pool, fill those frames first
    if (bp_mgmt->framesCount < bm->numPages)
    {
        frame = allocateEmptyFrame(bp_mgmt, pageNum);
    }
    else
    {
        // Replace pages from the frame using LRU
        frame = replacePage(bm, bp_mgmt, &fh, pageNum);
        if (frame == NULL)
        {
            return RC_WRITE_FAILED;
        }
    }

    ensureCapacity((pageNum + 1), &fh);
    if (readBlock(pageNum, &fh, frame->data) != RC_OK)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }
    bp_mgmt->readNumber++;

    // Update the page frame and its data
    page->pageNum = pageNum;
    page->data = frame->data;

    return RC_OK;
}

BM_PageFrame *findFrameInBufferPool(BM_PoolInfo *bp_mgmt, const PageNumber pageNum)
{
    BM_PageFrame *frame = bp_mgmt->head;
    do
    {
        if (frame->pageNumber == pageNum)
        {
            return frame;
        }
        frame = frame->nextFrame;
    } while (frame != bp_mgmt->head);
    return NULL;
}

void updatePageAndFrame(BM_PageHandle *const page, BM_PageFrame *frame, const PageNumber pageNum, BM_PoolInfo *bp_mgmt)
{
    page->pageNum = pageNum;
    page->data = frame->data;

    frame->fixCount++;

    bp_mgmt->tail = bp_mgmt->head->nextFrame;
    bp_mgmt->head = frame;
}

BM_PageFrame *allocateEmptyFrame(BM_PoolInfo *bp_mgmt, const PageNumber pageNum)
{
    BM_PageFrame *frame = bp_mgmt->head;
    frame->pageNumber = pageNum;

    if (frame->nextFrame != bp_mgmt->head)
    {
        bp_mgmt->head = frame->nextFrame;
    }

    frame->fixCount++;
    bp_mgmt->framesCount++;
    return frame;
}

BM_PageFrame *replacePage(BM_BufferPool *const bm, BM_PoolInfo *bp_mgmt, SM_FileHandle *fh, const PageNumber pageNum)
{
    BM_PageFrame *frame = bp_mgmt->tail;
    do
    {
        if (frame->fixCount == 0)
        {
            if (frame->isDirty)
            {
                ensureCapacity(frame->pageNumber, fh);
                if (writeBlock(frame->pageNumber, fh, frame->data) != RC_OK)
                {
                    return NULL;
                }
                bp_mgmt->writeNumber++;
            }

            if (bp_mgmt->tail != bp_mgmt->head)
            {
                frame->pageNumber = pageNum;
                frame->fixCount++;
                bp_mgmt->tail = frame;
                bp_mgmt->tail = frame->nextFrame;
                return frame;
            }
            else
            {
                frame = frame->nextFrame;
                frame->pageNumber = pageNum;
                frame->fixCount++;
                bp_mgmt->tail = frame;
                bp_mgmt->head = frame;
                bp_mgmt->tail = frame->previousFrame;
                return frame;
            }
        }
        frame = frame->nextFrame;
    } while (frame != bp_mgmt->tail);

    return NULL;
}

/**
 * Method to pin the page with page number pageNum.
 */

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{

    switch (bm->strategy)
    {
    case RS_FIFO:
        FIFO(bm, page, pageNum);
        break;

    case RS_LRU:
        LRU(bm, page, pageNum);
        break;

    case RS_CLOCK:
        // pinPageFIFO(bm, page, pageNum);
        break;
    }
    return RC_OK;
}

/**
 * Method to unpin the page. The pageNum field of page is used to figure out which page to unpin.
 */
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    BM_PoolInfo *bpInfo = bm->mgmtData; // initializing the pointer variable bpInfo to point to the memory location of mgmtData of bm.

    for (int i = 0; i < bm->numPages; i++)
    {
        BM_PageFrame *pageFrame = &(bpInfo->bufferPool[i]); // initializing page pointer to point to ith address of buffer pool
        if (pageFrame->pageNumber == page->pageNum)         // if the page frame page number and the requested page number is same, the decrement the fix count to unpin the page
        {
            pageFrame->fixCount--; // decrements the fixcount
            return RC_OK;          // returns successful response
        }
    }
    return RC_OK;
}

/**
 *  Method to mark a page as dirty
 **/
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // If bufferpool or page information  is null returns error code
    if (bm == NULL || page == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    BM_PoolInfo *bpInfo = bm->mgmtData; // initializing the pointer variable bpInfo to point to the memory location of mgmtData of bm.

    for (int i = 0; i < bm->numPages; i++)
    {
        BM_PageFrame *pageFrame = &(bpInfo->bufferPool[i]); // initializing page frame pointer to point to ith address of buffer pool
        if (pageFrame->pageNumber == page->pageNum)         // if the page frame page number and the requested page number is same, marks the page as dirty
        {
            pageFrame->isDirty = true; // setting isDirty flag to true
            return RC_OK;              // returns successful respone
        }
    }

    return RC_PAGE_NOT_FOUND;
}

/**
 * Method to write the current content of the page back to the page file on disk
 */
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    BM_PoolInfo *bpInfo = bm->mgmtData; // initializing the pointer variable bpInfo to point to the memory location of mgmtData of bm.
    for (int i = 0; i < bm->numPages; i++)
    {
        if (((BM_PageFrame *)&bpInfo->bufferPool[i])->pageNumber == page->pageNum) // initializing pointer to point to ith address of buffer pool and checking if the page number matches with the requested page number
        {
            BM_PageFrame *targetPage = &(bpInfo->bufferPool[i]); // initializing target page pointer to point to ith address of buffer pool
            SM_FileHandle fHandle;
            RC rc;
            rc = openPageFile(bm->pageFile, &fHandle); // opens an existing file
            if (rc != RC_OK)
            {
                return rc; // returns error code if response is unsuccessful
            }
            rc = writeBlock(page->pageNum, &fHandle, page->data); // write a page to disk at an absolute position
            if (rc != RC_OK)
            {
                closePageFile(&fHandle); // closes the file
                return rc;               // returns error code if response is unsuccessful
            }

            targetPage->isDirty = false; // target page isDirty flag is set to flase

            bpInfo->writeNumber++; // increment the write number of bufferpool info

            rc = closePageFile(&fHandle); // close the page file
            if (rc != RC_OK)
            {
                return rc; // returns error code if response is unsuccessful
            }
            return RC_OK;
        }
    }
    return RC_OK;
}

/*Page Management Functions - END*/

/*Statistics Functions - BEGIN*/

/**
 * Method returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame.
 * An empty page frame is represented using the constant NO PAGE
 */
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    BM_PoolInfo *bpInfo = bm->mgmtData;                        // initializing the pointer variable buffPoolInfo to point to the memory location of mgmtData of bm.
    int *pageNums = (int *)malloc(bm->numPages * sizeof(int)); // dynamically allocationg memory to page nums and returns pointer to allocated memory

    for (int i = 0; i < bm->numPages; i++)
    {
        int pageNum = ((BM_PageFrame *)&bpInfo->bufferPool[i])->pageNumber; // initializing pointer to point to ith address of buffer pool and assigninig its pageNumber to pageNum
        pageNums[i] = (pageNum == NO_PAGE) ? NO_PAGE : pageNum;             // When the page frame is empty NO_PAGE is returend(-1)
    }
    return pageNums; // returns array of page numbers of size equal to the number of frames in buffer pool
}

/**
 * Method returns an array of bools (of size numPages) where the ith elementis TRUE if the page stored in the ith page frame is dirty.
 * Empty page frames are considered as clean
 */
bool *getDirtyFlags(BM_BufferPool *const bm)
{
    BM_PoolInfo *bpInfo = bm->mgmtData;                        // initializing the pointer variable buffPoolInfo to point to the memory location of mgmtData of bm.
    bool *dFlags = (bool *)malloc(bm->numPages * sizeof(int)); // dynamically allocationg memory to dirty flags and returns pointer to allocated memory

    for (int i = 0; i < bm->numPages; i++)
    {
        dFlags[i] = ((BM_PageFrame *)&bpInfo->bufferPool[i])->isDirty; // initializing pointer to point to ith address of buffer pool and assigninig its isDirty to dirtyFlags array
    }
    return dFlags; // returns array of dirty flags of size equal to the number of frames in buffer pool
}

/**
 * Method to return an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame.
 * Return 0 for empty page frames.
 */
int *getFixCounts(BM_BufferPool *const bm)
{
    BM_PoolInfo *bpInfo = bm->mgmtData;                       // initializing the pointer variable bpInfo to point to the memory location of mgmtData of bm.
    int *fCounts = (int *)malloc(bm->numPages * sizeof(int)); // dynamically allocationg memory to fix counts flags and returns pointer to allocated memory
    for (int i = 0; i < bm->numPages; i++)
    {
        fCounts[i] = ((BM_PageFrame *)&bpInfo->bufferPool[i])->fixCount; // initializing pointer to point to ith address of buffer pool and assigninig its fixCount to fixCount array
    }
    return fCounts; // returns array of fixcounts of size equal to the number of frames in buffer pool
}

/**
 * Method to return the number of pages that have been read from disk since a buffer pool has been initialized.
 */
int getNumReadIO(BM_BufferPool *const bm)
{
    return ((BM_PoolInfo *)bm->mgmtData)->readNumber;
}

/**
 * Method to return the number of pages written to the page file since the buffer pool has been initialized
 */
int getNumWriteIO(BM_BufferPool *const bm)
{
    return ((BM_PoolInfo *)bm->mgmtData)->writeNumber;
}

/*Statistics Functions - END*/
