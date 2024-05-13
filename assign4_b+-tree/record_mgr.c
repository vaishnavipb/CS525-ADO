#include "string.h"

#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"
#include "rm_serializer.c"

SM_FileHandle file_handle; 

typedef struct PageSlot {
    int slotNum;
    struct PageSlot *next;
} PageSlot;

// Define a struct to hold scan management information
typedef struct RM_ScanManagement {
    Expr *condition;  // The scan condition associated with every scan
    int currentPage;  // The current page being scanned
    int currentSlot;  // The current slot being scanned
	PageSlot *freeSlots;  // Added field to store free slots

} RM_ScanManagement;


typedef struct REL_Manager {
	BM_PageHandle pageHandle;
	BM_BufferPool bufferpool;
	PageDirectory *pageDir;
    int tuples;
    Schema *schema;
} REL_Manager; 

// global variables
SM_FileHandle fHandle; 
BM_BufferPool *bm; 
BM_PageHandle *page; 
RecordNode *head = NULL; 

int tuples = 0; 
int recSize; 
int maxSlotsPerPage; 
int maxPageDirsPerPage; 

void * parseKeyInfo(Schema *schema, char *keyInfo);
char * serializePageDirectory(PageDirectory *pd);
RecordNode * deserializeRecords(Schema *schema, char *recordStr);
Schema *deserializeSchema(char *schemaData);

// Bookkeeping for scans

// table and manager -Begin

/**
 * Method to initialize record manager
 * */
RC initRecordManager(void *mgmtData)
{
   return RC_OK;
}

/**
 * Method to shut down record manager
 * */
RC shutdownRecordManager()
{
    return RC_OK;
}
/**
 * Method to create table with the name and schema provided
 * */
RC createTable(char *name, Schema *schema)
{
    char *schemaInfo = serializeSchema(schema);
    PageDirectory *pd = createPageDirectoryNode(2);
    char *pdInfo = serializePageDirectory(pd);
    createPageFile(name);
    openPageFile(name, &fHandle);
    writeBlock(0, &fHandle, schemaInfo);
    ensureCapacity(2, &fHandle);
    writeBlock(1, &fHandle, pdInfo);
    closePageFile(&fHandle);
    tuples = 0;
    recSize = getRecordSize(schema) + sizeof(int) + sizeof(int) + 2 + 2 + 2 + 3 + 1 + 3 + 1; 
    maxSlotsPerPage = 100; 
    maxPageDirsPerPage = PAGE_SIZE / strlen(pdInfo);
    free(schemaInfo);
    free(pd);
    free(pdInfo);
    return RC_OK;
}

/**
 * Method to open table with the specied name and store the information of schema to rm table data
 * */
RC openTable(RM_TableData *rel, char *name)
{
    bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    
    initBufferPool(bm, name, 100, RS_LRU, NULL);
    pinPage(bm, page, 0);
    rel->name = name;

    Schema *schema = deserializeSchema(page->data);
    rel->schema = schema;
    unpinPage(bm, page);
    pinPage(bm, page, 1);

    PageDirectoryCache *pageDirectoryCache = (PageDirectoryCache *)malloc(sizeof(PageDirectoryCache));

    pageDirectoryCache->count = 0;
    pageDirectoryCache->front = NULL;
    pageDirectoryCache->rear = NULL;

    char *token = strtok(page->data, "\n");
    while (token != NULL) {
        PageDirectory *pd = (PageDirectory *)malloc(sizeof(PageDirectory));
        
        sscanf(token + 1, "%d", &pd->pageNum);
        sscanf(token + 6, "%d", &pd->count);
        sscanf(token + 11, "%d", &pd->firstFreeSlot);
        pd->next = NULL;
        if (pageDirectoryCache->front == NULL) {
            pageDirectoryCache->front = pd;
        } else {
            pageDirectoryCache->rear->next = pd;
        }
        pageDirectoryCache->rear = pd;
        pageDirectoryCache->count++;

        token = strtok(NULL, "\n");
    }

    rel->mgmtData = pageDirectoryCache;

    unpinPage(bm, page);

    return RC_OK;
}


/**
 * Method to close the table and free up memory allocated
 * */
RC closeTable(RM_TableData *rel) {
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    RC rc = pinPage(bm, page, 1);

    PageDirectoryCache *pageDirectoryCache = rel->mgmtData;
    PageDirectory *p = pageDirectoryCache->front;

    // Directly serialize the page directories and write to the page data
    char *pageData = page->data;
    int offset = 0;

    while (p != NULL) {
        // Serialize each page directory
        char *serializedPD = serializePageDirectory(p);
        int pdLength = strlen(serializedPD);

        // Write the serialized page directory to the page data
        memcpy(pageData + offset, serializedPD, pdLength);
        offset += pdLength;

        // Add a newline character as a separator
        pageData[offset] = '\n';
        offset++;

        free(serializedPD);
        p = p->next;
    }

    // Null-terminate the page data
    pageData[offset] = '\0';

    markDirty(bm, page);
    unpinPage(bm, page);
    forcePage(bm, page);
    free(page);

    shutdownBufferPool(bm);
    freeSchema(rel->schema);
    free(pageDirectoryCache);

    return RC_OK;
}

/**
 * Method to delete the table with the specified name
 * */
RC deleteTable(char *name)
{
    return destroyPageFile(name); //deletes the table
}

int getNumTuples(RM_TableData *rel)
{
    return tuples;
}

// table and manager -End

RC insertRecord (RM_TableData *rel, Record *record)
{

Schema *schema = rel->schema;
    PageDirectoryCache *pageDirectoryCache = rel->mgmtData;
    int lastPageNum;
    int lastFreeSlot;
    int lastPageCount;
    PageDirectory *lastPD;
    PageDirectory *p = pageDirectoryCache->front;
    
    while (p != NULL) {
        if (p->count < maxSlotsPerPage) {
            lastPD = p;
            break;
        }
        p = p->next;
    }
    
    if (p == NULL) {
        lastPageNum = pageDirectoryCache->rear->pageNum + 1;
        PageDirectory *pd = createPageDirectoryNode(lastPageNum);
        
        if (pageDirectoryCache->count % maxPageDirsPerPage == 0) {
            // this new page will store another page directories
            lastPageNum = pageDirectoryCache->rear->pageNum + 1;
            
            BM_PageHandle *page = MAKE_PAGE_HANDLE();
            pinPage(bm, page, lastPageNum);
            
            char *pdStr = serializePageDirectory(pd);
            int pdStrLen = strlen(pdStr);
            memcpy(page->data, pdStr, pdStrLen);
            
            free(pdStr);
            markDirty(bm, page);
            unpinPage(bm, page);
            forcePage(bm, page);
            free(page);
        }
        
        pageDirectoryCache->rear->next = pd;
        // pd->pre = pageDirectoryCache->rear;
        pageDirectoryCache->rear = pd;
        pageDirectoryCache->count = pageDirectoryCache->count + 1;
        lastPD = pageDirectoryCache->rear;
    }
    
    lastPageNum = lastPD->pageNum;
    lastFreeSlot = lastPD->firstFreeSlot;
    lastPageCount = lastPD->count;
    
    record->id.page = lastPageNum;
    record->id.slot = lastFreeSlot;
    int offset = record->id.slot * recSize;
    
    char *recordStr = serializeRecord(record, schema);
    int recordStrLen = strlen(recordStr);
    
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    pinPage(bm, page, lastPageNum);
    
    memcpy(page->data + offset, recordStr, recordStrLen);
    
    free(recordStr);
    markDirty(bm, page);
    unpinPage(bm, page);
    forcePage(bm, page);
    free(page);
    
    lastPD->count = lastPD->count + 1;
    lastPD->firstFreeSlot = lastPD->firstFreeSlot + 1;
    
    rel->mgmtData = pageDirectoryCache;
    tuples++;
    return RC_OK;
}
RC deleteRecord(RM_TableData *rel, RID id) {
    PageDirectoryCache *pageDirectoryCache = rel->mgmtData;
    PageDirectory *p = pageDirectoryCache->front;

    pinPage(bm, page, p->pageNum);
    for (; p != NULL; p = p->next) {
        if (p->pageNum == id.page) {
            Record *record = (Record *)malloc(sizeof(Record));
            getRecord(rel, id, record);
            char *recordStr = serializeRecord(record, rel->schema);
            strncpy(page->data + (recSize * id.slot), recordStr, recSize);
            freeRecord(record);
            break;
        }
    }
    markDirty(bm, page);
    unpinPage(bm, page);
    forcePage(bm, page);
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record)
{
    PageDirectoryCache *pageDirectoryCache = rel->mgmtData;
    PageDirectory *p = pageDirectoryCache->front;
    Schema *schema = rel->schema;

    for (; p != NULL; p = p->next) {
    if (p->pageNum == record->id.page) {
        pinPage(bm, page, p->pageNum);

        int offset = recSize * record->id.slot;
        char *recordData = (char *)malloc(recSize * sizeof(char));
        memset(recordData, 0, recSize * sizeof(char));
        memcpy(recordData, record->data, recSize);

        char *recordStr = serializeRecord(record, schema);
        BM_PoolInfo* pageCache = bm->mgmtData;
        strcpy(page->data + offset, recordStr);

        markDirty(bm, page);
        unpinPage(bm, page);
        forcePage(bm, page);

        free(recordData);
        free(recordStr);
        break;
        }
    }
    return RC_OK; 
}

RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    Schema *schema = rel->schema;

    // Pin the page containing the record
    pinPage(bm, page, id.page);
    
    head = deserializeRecords(rel->schema, page->data);
    RecordNode *p = head;
    while (p != NULL) {
        if (id.page == p->rid.page && id.slot == p->rid.slot) {
            record->data = (char *)malloc(strlen(p->data) + 1);
            memcpy(record->data, p->data, strlen(p->data) + 1);
            return RC_OK; 
        }
        p = p->next;
    }
    unpinPage(bm, page);
    return RC_PAGE_NOT_FOUND;
}

// scans
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    scan->mgmtData=(RM_ScanManagement*)malloc(sizeof(RM_ScanManagement));
    RM_ScanManagement *sm=(RM_ScanManagement*)scan->mgmtData;

    sm->currentPage=2; 
    sm->currentSlot=0;
    sm->condition=cond;

    scan->rel=rel;
    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record)
{
    RM_TableData *rel=(RM_TableData*)scan->rel;
    RM_ScanManagement *sm=(RM_ScanManagement *)scan->mgmtData;

    PageDirectoryCache *pageDirectoryCache = rel->mgmtData;
    int maxPageNum = pageDirectoryCache->rear->pageNum;
    int currentPage= sm->currentPage;
    int currentSlot=sm->currentSlot;

    while(sm->currentPage<=maxPageNum){
        if(sm->currentSlot>=maxSlotsPerPage){
            sm->currentSlot=0;
            sm->currentPage++;
            if(sm->currentPage % (maxPageDirsPerPage + 1) == 0) {
                sm->currentPage++;
            }
            continue;
        }
        RID rid;
        rid.page=sm->currentPage;
        rid.slot=sm->currentSlot;
        getRecord(rel,rid,record);

        sm->currentSlot++;
        if(sm->condition==NULL){
            return RC_OK;
        }
        else{
            Value *result = (Value *) malloc(sizeof(Value));
            evalExpr(record,rel->schema,sm->condition, &result);
            if(result!=NULL && result->v.boolV!=0){
                freeVal(result);
                return RC_OK;
            }
            freeVal(result);
        }
        
    }
    sm->currentSlot--;

    return RC_RM_NO_MORE_TUPLES;
}

RC closeScan(RM_ScanHandle *scan)
{
    scan->mgmtData = NULL;
    return RC_OK;
}
// dealing with schemas - Begin

/**
 * Method to get the size of the records for the specified schema 
 * */
int getRecordSize(Schema *schema)
{
    int recSize = 0; //initalizing the record size to 0
    for (int i = 0; i < schema->numAttr; i++) // iterating through the attributes present in the schema
    {
        switch (schema->dataTypes[i])
        {
        case DT_INT:
            recSize += sizeof(int); //gets the size of int and add to record size
            break;
        case DT_STRING:
            recSize += sizeof(char) * schema->typeLength[i]; // gets the size of string and add to record size
            break;
        case DT_BOOL:
            recSize += sizeof(bool); //gets the size of bool and add to record size
            break;
        case DT_FLOAT:
            recSize += sizeof(float); // gets the size of float and add to record size
            break;
        default:
            break;
        }
    }
    return recSize; //returning record size
}

/**
 * Method to create a new schema with the specified attributes
 * */
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *schema = (Schema *)malloc(sizeof(Schema));//allocating memory dynamically to schema
    //assigning the fields passed to the method to the schema attributes  
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    return schema; // returs the new schema created
}

/**
 * Method to free schema resources 
 * */
RC freeSchema(Schema *schema)
{
    for (int k = 0; k < schema->numAttr; k++)
        {
            free(schema->attrNames[k]); //frees the memory allocated for attribut names
        }
    //frees the memory allocated for the rest of the resources in  schema
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);
    return RC_OK; // returns successful response
}

// dealing with schemas - End

// dealing with records and attribute values - Begin

/**
 * Method to create record for the specified schema
 * */
RC createRecord(Record **record, Schema *schema)
{
    (*record) = (Record *)malloc(sizeof(Record)); //dymaic memory allocation to the record
    (*record)->data = (char *)malloc(sizeof(char) * getRecordSize(schema)); // allocates memory based on the size of the records in the schema provided
    if (record == NULL){
        return RC_CREATE_RECORD_FAILED; //returns negative return code if the record creation failed
    }
    return RC_OK; //returns successful return code
}

/**
 * Method to free the resources allocated for the record
 * */
RC freeRecord(Record *record)
{
    free(record->data); //frees up the record data
    free(record); // frees up the record
    return RC_OK; // returns successful resposne
}

/**
 * Method to get the value of an attribute of a record
 * */
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    int position = 0; //intialize the starting offset position
    RC rc = attrOffset(schema, attrNum, &position); // updates the position based on the attrNum
   
    Value *val = (Value*)malloc(sizeof(Value)); //allocating memory to store the value

    DataType type = schema->dataTypes[attrNum]; //finding the type of attrNum
    val->dt = type; // setting the datatype to val
    
    switch(type){   
        case DT_STRING:
        {
            int length = sizeof(char)*schema->typeLength[attrNum]; //setting the length of the string based on attrNum
            val->v.stringV = (char*)malloc(length + 1); // allocating memory dynamically to store the record
            memcpy(val->v.stringV, record->data + position, length);//copies data from record to value
            val->v.stringV[length] = '\0'; //settng the null terminator at the end of the array
            break;
        }
        case DT_INT:
        {
            void *attrData = malloc(sizeof(int)); // memory allocation
            memcpy(attrData, record->data + position, sizeof(int)); // copies data from record to memory location pointed by attrData
            val->v.intV = atoi(attrData); // converting to integer and assigning to v.intV
            free(attrData); //freeup attrData
            break;
        }
        case DT_FLOAT:
        {
            void *attrData = malloc(sizeof(float)); // memory allocation
            memcpy(attrData, record->data + position, sizeof(float)); // copies data from record to memory location pointed by attrData
            val->v.floatV = atof(attrData); // converting to float and assigning to v.floatV
            free(attrData); // freeup attrData
            break;
        }
        case DT_BOOL:
        {
            void *attrData = malloc(sizeof(bool));// memory allocation
            memcpy(attrData, record->data + position, sizeof(bool)); // copies data from record to memory location pointed by attrData
            val->v.boolV = (strcmp(attrData, "true") == 0 || strcmp(attrData, "TRUE") == 0 || strcmp(attrData, "1") == 0); //coverting to bool and assigning to v.boolV
            free(attrData); // freeup attrData
            break;
        }
        default:
        {
            free(val); //frees up value
            return RC_INVALID_DATATYPE;//retuns unsuccessful return code incase of negative return code
        }
    }
    *value = val; //updating the pointer to point to the newly created value
    return RC_OK; // returns successful response
}

/**
 * Method to set attribute value to a record
 * */
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    char data[sizeof(int) + 1];
    memset(data, '0', sizeof(char)*4);
    int offset = 0;
    if(attrOffset(schema, attrNum, &offset) != RC_OK) {
            return RC_NOT_OK;
    }    
    switch (value->dt){
       case DT_STRING:
        sprintf(record->data + offset, "%s", value->v.stringV);
        break;
    case DT_INT:
        convertPage(3, value->v.intV, data);
        sprintf(record->data + offset, "%s", data);        
        break;
    case DT_FLOAT:
        sprintf(record->data + offset,"%f" ,value->v.floatV);
        break;
    case DT_BOOL:
        convertPage(1, value->v.boolV, data);
        sprintf(record->data + offset,"%s" ,data);
        break;
    default:
        return RC_INVALID_DATATYPE; 
    }
   
    return RC_OK;
}
PageDirectory *
createPageDirectoryNode(int pageNum) {
    PageDirectory *pd = (PageDirectory*)malloc(sizeof(PageDirectory));
    pd->pageNum = pageNum;
    pd->count = 0;
    pd->firstFreeSlot = 0;
    // pd->pre = NULL;
    pd->next = NULL;
    return pd;
}


char * serializePageDirectory(PageDirectory *pd)
{
	VarString *result;
	MAKE_VARSTRING(result);
    int attrSize = sizeof(int);
    char data[attrSize + 1];
    memset(data, '0', sizeof(char)*4);
 
	convertPage(3, pd->pageNum, data);
	APPEND(result, "[%s-", data);

	memset(data, '0', sizeof(char)*4);
	convertPage(3, pd->count, data);

	APPEND(result, "%s-", data);

	memset(data, '0', sizeof(char)*4);
	convertPage(3, pd->firstFreeSlot, data);

	APPEND(result, "%s]", data);
	APPEND_STRING(result,"\n");
	RETURN_STRING(result);
}

RecordNode *deserializeRecords(Schema *schema, char *recordStr) 
{
    char *token;
    char *a = strdup(recordStr);
    token = strtok(a, "\n");
    
    RecordNode *head = NULL;
    RecordNode *p = NULL;
    int i = 0;
    int recSize = getRecordSize(schema);
    while (token != NULL) {
        RecordNode *node = (RecordNode *)malloc(sizeof(RecordNode));
        node->data = (char*)calloc(recSize + 1, sizeof(char));
        int offset = 14;
		for (int i = 0; i < schema->numAttr; i++) {
			int result = 0;
			attrOffset(schema, i, &result);

			switch (schema->dataTypes[i]) {
				case DT_INT:
				case DT_BOOL: {
					int val = 0;
					memcpy(&val, token + offset, sizeof(int));
					memcpy(node->data + result, &val, sizeof(int));
					offset += sizeof(int);
					break;
				}
				case DT_STRING: {
					int len = schema->typeLength[i];
					memcpy(node->data + result, token + offset, len);
					offset += len;
					break;
				}
				default:
					break;
			}
			offset += 2 + strlen(schema->attrNames[i]);
		}
		int page = atoi(token + 1);
		int slot = atoi(token + 6);
        node->rid.page = page;
        node->rid.slot = slot;
        node->next = NULL;
        if (p != NULL) {
			p->next = node;
		} else {
			head = node;
		}
        p = node;
        token = strtok(NULL, "\n");
    }
    return head;
}


Schema *deserializeSchema(char *schemaData) {
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    char *numAttrStr = schemaData;
    while (*numAttrStr != '<') {
        numAttrStr++;
    }
    numAttrStr++;
    int numAttr = atoi(numAttrStr);
    schema->numAttr = numAttr;

    // Allocate memory for attribute names, data types, and type lengths
    char **attrNames = (char **) malloc(sizeof(char*) * numAttr);
    DataType *dataTypes = (DataType *) malloc(sizeof(DataType) * numAttr);
    int *typeLength = (int *) malloc(sizeof(int) * numAttr);

    // Get attribute info
    char *attrInfo = numAttrStr;
    while (*attrInfo != '(') {
        attrInfo++;
    }
    attrInfo++;

    char *token = strtok(attrInfo, ":, )");
    for (int i = 0; i < numAttr && token != NULL; i++) {
        int size = strlen(token) + 1;
        attrNames[i] = (char *)malloc(size * sizeof(char));
        strcpy(attrNames[i], token);

        // Get data type
        token = strtok(NULL, ":, )");
        int tokenSize = strlen(token);
        if (token[0] == 'I') {
            dataTypes[i] = DT_INT;
            typeLength[i] = 0;
        } else if (token[0] == 'F') {
            dataTypes[i] = DT_FLOAT;
            typeLength[i] = 0;
        } else if (token[0] == 'B') {
            dataTypes[i] = DT_BOOL;
            typeLength[i] = 0;
        } else if (token[0] == 'S') {
            dataTypes[i] = DT_STRING;
            if (tokenSize > 6 && token[6] == '[') {
                char *length = token + 7;
                typeLength[i] = atoi(length);
            } else {
                typeLength[i] = 0;
            }
        }
        token = strtok(NULL, ":, )");
    }

    schema->dataTypes = dataTypes;
    schema->attrNames = attrNames;
    schema->typeLength = typeLength;

    // Get keys info
    char *keyInfo = attrInfo;
    while (*keyInfo != '{') {
        keyInfo++;
    }
    keyInfo++;
    parseKeyInfo(schema, keyInfo);

    return schema;
}

void * parseKeyInfo(Schema *schema, char *keyInfo)
{
	int numAttr = schema->numAttr;
    int *keyAttrs = (int *) malloc(sizeof(int) * numAttr);
    int index = -1;

    for(int i = 0; i < numAttr; i++) {
        if(strcmp(schema->attrNames[i], keyInfo) == 0) {
            index = i;
            break;
        }
    }
    for(int i= 0; i < numAttr; ++i)
    {
       keyAttrs[i] = index;
        
    }
    schema->keyAttrs = keyAttrs;
	schema->keySize = 1;
}
