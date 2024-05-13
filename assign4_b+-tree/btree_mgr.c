#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include "dberror.h"
#include "tables.h"
#include "btree_mgr.h"
#include "buffer_mgr.h"
#include <stddef.h>
#include <string.h>

int scanPosition = 0; // setting the scan position to 0
static uint8_t currentInstanceIndex = 0; // initiating  current instance index
static BTreeManager btreeInstances[MAX_NODES];// setting the size of btree instances 

// function declarations which are used in deleteKey method
RC deleteKeyHelper(TreeNode *node, int val);
RC deleteKeyFromLeaf(TreeNode *node, int val);
int getChildIndex(TreeNode *node, int val);
// function declarations which are used in insertKey method
TreeNode *addNodeToTree(BTreeHandle *tree, TreeNode *node, int val, RID rid);
// function declarations which are used in nextExtry method 
int storeResultValues(RID *result, int page, int slot);
TreeNode *traverseToNextNode(TreeNode *node, BT_ScanHandle *handle);

// Method to initialize the btree index manager
RC initIndexManager(void *mgmtData) {
    return RC_OK; //returns successful response
}

// Method to shutdown the btree index manager
RC shutdownIndexManager() {
    return RC_OK;//returns successful response
}

// Method to create B+ tree index
RC createBtree (char *idxId, DataType keyType, int n){
    BTreeHandle *tree = malloc(sizeof(BTreeHandle)); // allocating memory for the btree handle
    tree->mgmtData = (TreeNode *)malloc(sizeof(TreeNode)); // allocating memory for treenode which will be used as the mgmtData 
    ((TreeNode *)(tree->mgmtData))->numValues = 0; // initializing the numValues in the root node of the tree to 0 
    ((TreeNode *)(tree->mgmtData))->isLeaf = true; // as there are no leafnodes yet, setting the root node as leaf node
    tree->keyType = keyType; // stores the datatype of the key
    tree->idxId = idxId; // storing the id for the tree
    btreeInstances[currentInstanceIndex++] = (BTreeManager){tree, 0, 0}; // storing the tree handle to the array of B Tree instances and increment the count of current instances.
    return RC_OK;//return successful response
}

// Method to open BTree at the specified index
RC openBtree(BTreeHandle **tree, char *idxId) {
    for (int i = 0; i < currentInstanceIndex; i++) { //iterating through all the instances of the B+ tree
        if (!strcmp(idxId, btreeInstances[i].bth->idxId)) { //checking if the current tree index matched with the id given
            *tree = btreeInstances[i].bth; //when there is a match, treehandle to poited to the matched b+ tree
            return RC_OK; // return successful response
        }
    }
}

// Method to close the b+ tree 
RC closeBtree(BTreeHandle *tree) {
    return RC_OK;//return successful response
}

// Method to delete the b+ tree on the specified index
RC deleteBtree (char *idxId){
    for (int i = 0; i < currentInstanceIndex; i++) {  //iterating through all the instances of the B+ tree
        if (strncmp(idxId, btreeInstances[i].bth->idxId, strlen(idxId)) == 0) { //checking if the current tree index matched with the id given
            free(btreeInstances[i].bth); //free up the memory allocated to b+ tree handle
            btreeInstances[i] = btreeInstances[--currentInstanceIndex]; // moving the last instance  of the b+ tree to the current position to fill the gap
            return RC_OK;// return successful response
        }
    }
}

// Method to get the number of nodes in b+ tree 
RC getNumNodes (BTreeHandle *tree, int *result){
    const ptrdiff_t index = tree - btreeInstances[0].bth;// calculating the diffence between the given tree handle and the first tree handle 
    *result = btreeInstances[index].numNodes; // dereferencing the result to store the number of nodes of the in the b+ tree
    return RC_OK; // return successful response
}

//Method to get number of entries in b+ tree
RC getNumEntries(BTreeHandle *tree, int *result) {
    const ptrdiff_t index = tree - btreeInstances[0].bth;// calculate the index of the BTreeHandle with the index of b tree instances
    *result = btreeInstances[index].numEntries; // retrieve the number of entries from the B+ Tree instance at the calculated index and store in result
    return RC_OK;// return successful response
}

// Method to get the datatype of the key
RC getKeyType (BTreeHandle *tree, DataType *result) {
    *result = tree->keyType; //fetching the keytype from the tree handle and store in the result 
    return RC_OK;// return successful response
}

// Method to find the key in B+ tress which returns record ID with the search key 
RC findKey (BTreeHandle *tree, Value *key, RID *result){
    TreeNode *node = (TreeNode *)tree->mgmtData; // accessing the management data from the tree and assigning it to the node 
    int val = key->v.intV; // getting the integer value of the key to be searched
	// iterating down the tree starting from the root to find the appropriate leaf node
    while (!node->isLeaf) {//continues till the leaf node is reached 
        int childIndex = 0;// initialize child index
        while (childIndex < node->numValues && val >= node->values[childIndex]) {// Determinig the child node to follow based on the key value
            childIndex++; // increment the child index to move to the next child if the key is greater than or equal to the current node value
        }
        node = node->children[childIndex];//move to the child node at the index
    }
	// iterate through the number of values in the leafnode
    for (int i = 0; i < node->numValues; i++) {
        if (node->values[i] == val && !node->isDeleted[i]) { // checking if the value matches the value in the current node and check is node is not deleted
            *result = node->rids[i]; // assigning the record id to the result
            return RC_OK; // returns successful response 
        }
    }
    return RC_IM_KEY_NOT_FOUND;    //returns error code if key is not found
}

// Method to update the node count
RC updateNodeCount (BTreeHandle *tree){
    for (int i = 0; i < currentInstanceIndex; i++) { // iterating through the current b+ tree instances 
        if (btreeInstances[i].bth == tree) { // checking if the specified tree matched the b_tree instances
            btreeInstances[i].numNodes++; // incrementing the number of nodes for the current B+ tree
            return RC_OK; // return successful response 
        }
    }
}

// Method to update the entry count
RC updateEntryCount (BTreeHandle *tree){
    for (int i = 0; i < currentInstanceIndex; i++) {// iterating through the current b+ tree instances
        if (btreeInstances[i].bth == tree) { // checking if the specified tree matched the b_tree instances
            btreeInstances[i].numEntries++;// incrementing the number of entries for the current B+ tree
            return RC_OK;// return successful response 
        }
    }
}

// Method to insert into leaf node with one value
TreeNode* insertIntoLeafWithOneValue(BTreeHandle* tree, TreeNode* node, RID rid, int val) {
    node->numValues++; // increase the count of the number of values 
    if (node->values[0] > val) {// check if the at the first index is more than the value specified
		node->values[1] = node->values[0]; // assign the value at the index 0 to index 1
        node->rids[1] = node->rids[0]; // assign the record id at the index 0 to index 1
        node->values[0] = val; // assign the value passed to the method to the value at index 0
        node->rids[0] = rid; // assign the record passed to the method to the index 0 
    } else {
    	node->values[1] = val; // assign the value to the index 1 value
        node->rids[1] = rid; // assign the rid to the recird id at indes 1
	}
	updateEntryCount(tree); // update the entry count
}

// Method to split the leaf nodes in the B+ treeS
TreeNode* splitLeaf(BTreeHandle *tree, TreeNode *node, RID rid, int val){
	updateNodeCount(tree); //update the node count
	TreeNode* newRoot = (TreeNode *)malloc(sizeof(TreeNode)); // allocate memory to the newRoot based on the size of tree node
	newRoot->numValues = 1; // setting new root number of values to 1 

	TreeNode* right = (TreeNode *)malloc(sizeof(TreeNode)); // allocate memory to the right node based on the size of tree node
	right->isLeaf = true; // set isLeaf flag to true for the right node
	right->numValues = 1; // setting the number of values to 1 for the right node
	right->children[2] = node->children[2]; // assigning 2 children tree nodes to the right nodes children
	node->children[2] = right; // setting the node children to the right node
	right->values[1] = 0; //setting the value at index 1 of the right  node to 0

	if (val < node->values[0]) { // check if the value of the node at index 0 is more than the val passed to method 
		right->values[0] = node->values[1]; // assign the value at index 1 of node to value at index 0 of right node
		right->rids[0] = node->rids[1]; // assign the record id at index 1 of node to record id at index 0 of right node
		node->values[1] = node->values[0];  // assign the value at index 0 of node to value at index 1 of node
        node->rids[1] = node->rids[0]; // assign the rid at index 0 of node to the rid at index 1 of node
        node->values[0] = val;  // assign the value passed to value at index 0 of node
        node->rids[0] = rid;  // assign the rid passed to the rid at index 0 of node
	} else {
		if (val < node->values[1]) { // check if the value of the node at index 1 is more than the val passed to method 
			right->values[0] = node->values[1];// assign the value at index 1 of node to value at index 0 of node
			right->rids[0] = node->rids[1];// assign the record id at index 1 of node to record id at index 0 of node
			node->values[1] = val; // assign the value passed to value at index 1 of node
			node->rids[1] = rid;// assign the rid passed to the rid at index 1 of node
		} else {
			right->values[0] = val;// assign the value passed to the value at index 0 of right node
			right->rids[0] = rid;// assign the rid passed to the rid at index 0 of right node
		}
	}

	newRoot->children[0] = node; // assign the node to the child at index 0 of new root
	newRoot->children[1] = right; // assign the right node to the child at index 1 of new root
	newRoot->values[0] = right->values[0]; // assign the value at index zero of the right node to the value at index 0 of the new root
	updateEntryCount(tree); // update the entry count
	return newRoot; //return new root
}

// Method to add a node to the B+-tree
TreeNode *addNodeToTree(BTreeHandle *tree, TreeNode *node, int val, RID rid){
	if (node->isLeaf) { // checks of the node is a leaf node
		if (node->numValues == 1) { //check if the number of values is 1 for the node
			return insertIntoLeafWithOneValue(tree, node, rid, val); // insert into leaf with one value
		} else {
			return splitLeaf(tree, node, rid, val); //split the leaf node
		}
	}
	TreeNode *root = NULL; // initialize root to null
	if(node->numValues==1){ //check of the number of values in node is 1
		root = addNodeToTree(tree, node->children[node->values[0] > val ? 0 : 1], val, rid);//add the node to the tree
		if (root != NULL) { //check if the root node is not null after adding the node to the root
			node->numValues++; // increment the number of values for the node
			if (root->values[0] < node->values[0]) { //check if the value at index 0 of root is less than the value at index 0 of the node 
				node->values[1] = node->values[0];//assign the node value at index 0 to node value at index 1
				node->values[0] = root->values[0]; // assign the root value at index 0 to node value at index 0
				node->children[2] = node->children[1]; // assign the node child at index 1 to the node child at index 2
				node->children[0] = root->children[0]; // assign the root child at index 0 to the node child at index 0
				node->children[1] = root->children[1]; //assign the root child at index 1 to the node child at index 1
			} else {
				node->values[1] = root->values[0]; // assign root value at index 0 to root value at index 1
				node->children[1] = root->children[0]; // assign root children at index 0 to node children at index 1
				node->children[2] = root->children[1]; // assign root children at index 1 to node children at index 2
			}
		}
		return NULL; // return null in no condition matches
	}
	int index = (node->values[0] > val) ? 0 : (node->values[1] > val) ? 1 : 2; //setting initial index based in the value of node at index 0 and 1
	root = addNodeToTree(tree, node->children[index], val, rid);// add a node to the tree and set it to root
	if (root == NULL) { //check if root is null
		return NULL; // return null
	} 
	else {
		updateNodeCount(tree);//update node count
		TreeNode *newRoot = (TreeNode *)malloc(sizeof(TreeNode)); //allocate memory to the newroot based on the tree node
		TreeNode *right = (TreeNode *)malloc(sizeof(TreeNode)); //allocate memory to the right node based on the tree node
		TreeNode *left = (TreeNode *)malloc(sizeof(TreeNode)); //allocate memory to the left node based on the tree node
		
		newRoot->numValues = 1; // setting the number of values of newRoot to 1
		right->numValues = 1; // setting the number of values of right to 1
		left->numValues = 1; // setting the number of values of left to 1
		int mid = (root->values[0] < node->values[0]) ? 0 : (root->values[0] < node->values[1]) ? 1 : 2; // setting the mid value based on the root and node values

		newRoot->values[0] = (mid == 0) ? node->values[0] : (mid == 1) ? root->values[0] : node->values[1]; //when mid == 0, assign value at index 0 of new root based on root/node values
		left->values[0] = (mid == 0) ? root->values[0] : node->values[0]; //when mid == 0, assign value at index 0 of left based on root/node values at index 0
		right->values[0] = (mid == 2) ? root->values[0] : node->values[1];//when mid ==2, assign value at index 0 of right based on root/node values 

		newRoot->children[0] = left; // set left node as the child of rew root
		newRoot->children[1] = right; // set right node as the chold of new root
		left->children[0] = node->children[0]; // assign children at index 0 of node to the children at index 0 of left node 
		left->children[1] = node->children[1]; // assign children at index 1 of node to the children at index 1 of left node 
		right->children[0] = node->children[1]; // assign children at index 1 of node to the children at index 0 of right node 
		right->children[1] = node->children[2];// assign children at index 2 of node to the children at index 1 of right node 
		return newRoot; // return new root
	}
}

//Method to insert key into B+ tree 
RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    TreeNode* root = (TreeNode*)tree->mgmtData; // accessing mgmt data and assigning it to tree node
    if (root->numValues == 0) { // checking if the root number of values is zero
        root->rids[0] = rid; // assigning record if to the record if of the root  
        root->values[0] = key->v.intV; // assigning the integer value to the value of root
        root->numValues = 1; // initializing the num values to 1
        updateNodeCount(tree); // update node count
        updateEntryCount(tree); // uodate enytry count
        return RC_OK; // return successful response
    }
    TreeNode *newRoot = addNodeToTree(tree, root, key->v.intV, rid); // adding a new node to the tree
    if (newRoot != NULL) { // checking if the new root is not equal to null
        tree->mgmtData = newRoot; // assigning newroot to tree
        updateNodeCount(tree);// update node count
    }
    return RC_OK; // return successful response
}

// Method to delete the key fron the b+ tree
RC deleteKey(BTreeHandle *tree, Value *key) {
    int val = key->v.intV; // getting the integer value of the key to be deleted
    TreeNode *node = (TreeNode *)tree->mgmtData;// accessing the mgmtData and assigning it to the tree node
    return deleteKeyHelper(node, val); // passing the node and val to the helper which retuns success/unsuccessful code
}

// Helper method to delete the node from  the B+ treee 
RC deleteKeyHelper(TreeNode *node, int val) {
    if (node->isLeaf) {  // If the current node is a leaf
        return deleteKeyFromLeaf(node, val); // deleting the node from the leaf 
    }
    int childIndex = getChildIndex(node, val); //if the node is not a leaf find the appropriate child index to continue searching for the key
    return deleteKeyHelper(node->children[childIndex], val); // recursively call deleteKeyHelper to go to child node
}

// Method to delete a key from a leaf node in B+ tree
RC deleteKeyFromLeaf(TreeNode *node, int val) {
    for (int i = 0; i < node->numValues; i++) { //iterate through the number of values in the node
        if (node->values[i] == val && !node->isDeleted[i]) { // check if the current key matches and is not deleted
            node->isDeleted[i] = true; // marks the key as deleted
            return RC_OK; // return successful response
        }
    }
    return RC_IM_KEY_NOT_FOUND; //if the key is not found, return  error code
}

// Method to determine the child index to follow for a given key value in a non-leaf node
int getChildIndex(TreeNode *node, int val) {
    if (val < node->values[0]) {// compare the key with the smallest value in the node
        return 0; //go to first child
    } else if (node->values[1] > 0 && val < node->values[1]) {
        return 1; // if child index is between the two node values, go to the second child
    } else {
        return 2; // else go to the third child 
    }
}

// Method to find the the leafnode at left
TreeNode *findLeftmostLeaf(TreeNode *node) {
    while (node != NULL && !node->isLeaf) //loops as long as node is not null and node is not a leaf
        node = node->children[0]; // move to the leftmost child, as B+-trees store the smallest keys on the left.
    return node; // return node
}

// Method to start scanning a B+ tree from the leftmost leaf sequentially to access all entries
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    BT_ScanHandle *scan = (BT_ScanHandle *)malloc(sizeof(BT_ScanHandle));//allocating memory for a new scan handle
    scan->tree = tree;   // assigning the tree to the scan handle for tracking the tree
    scan->mgmtData = findLeftmostLeaf(tree->mgmtData);  // the management data for the scan to the leftmost leaf of the tree, this is the starting point for the scan
    scanPosition = 1; // current the current page pos to 1
    *handle = scan; // the address of the newly created scan handle through the handle pointer
    return RC_OK; // returns successful response 
}

// Method to scan for the 
RC nextEntry(BT_ScanHandle *handle, RID *result) {
    TreeNode *node = (TreeNode *)handle->mgmtData; // accessing mgmt data from the scan handle and assigning it to the node

    if (node == NULL || scanPosition == -1) // Check if there are no more entries or scanPosition is -1
        return RC_IM_NO_MORE_ENTRIES; //return error code

    if (scanPosition == 1) { // First entry in the current node
        storeResultValues(result, node->rids[0].page, node->rids[0].slot);// store the result values
        scanPosition = 2; // updating the scan position to 2
    } else if (scanPosition == 2 && node->values[1] != 0) { // Second entry in the current node
        if (storeResultValues(result, node->rids[1].page, node->rids[1].slot)) {  //if the page and slot values are not 0, assigns them to result and returns true 
            node = node->children[2]; //assigning children at 2 position to node 
            handle->mgmtData = node; //assigning node to B+-tree scan hamdle management data 
            scanPosition = 1; //setting scan position to 1
        } else {
            scanPosition = -1; //setting scan position to -1 indcating no other entries to scan
        }
    } else { // Traverse to the next node
        node = traverseToNextNode(node, handle); // check for the next node
        if (node == NULL) // check if node is null
            return RC_IM_NO_MORE_ENTRIES; // return error code if there are no more entries
        storeResultValues(result, node->rids[0].page, node->rids[0].slot);
        scanPosition = 2; //setting scan position to 2
    }

    return RC_OK; // return successful response
}

//Method to store result values
int storeResultValues(RID *result, int page, int slot) {
    if (page < 0 || slot < 0) // If either page or slot is negative
        return 0;              // Return failure
    result->page = page;       // Store page value
    result->slot = slot;       // Store slot value
    return 1;                  // Return success
}

// Method to go to the next node
TreeNode *traverseToNextNode(TreeNode *node, BT_ScanHandle *handle) {
    node = node->children[2]; //assigning the child at 2 position node to the node  
    handle->mgmtData = node; // assigning node to the mgmtdata
    return node; // return node
}

// Method to close B+ tree scan
RC closeTreeScan (BT_ScanHandle *handle){
	return RC_OK; // return successful response
}

// Method to print the B+ treee for debugging and testing function
char *printTree(BTreeHandle *tree) {
    TreeNode *node = (TreeNode *)tree->mgmtData; // setting the tree node from the management data
    
    // defining a struct for stack nodes used in depth-first traversal
    typedef struct StackNode {
        TreeNode *node; //pointer to the tree node
        int level; //depth level in the btree 
    } StackNode;

    StackNode stack[100];  // Stack array to manage tree nodes during traversal
    int top = -1; // setting stack top index, -1 means the stack is empty
    stack[++top] = (StackNode){node, 0}; // push the root node onto the stack with level 0
	// looping through the stack until the stack is empty
    while (top != -1) {
        StackNode current = stack[top--]; // pop the top stack node
        TreeNode *currentNode = current.node;//current node being processed
        int level = current.level;// level of current node in tree
 		// add indentation based on the depth level
        for (int i = 0; i < level; i++)
            printf("   "); // adding 3 spaces for better redability
		// printing the current node level and values and adjusting the format
        printf("|_ (%d)[%d,%d]\n", level, currentNode->values[0], currentNode->values[1]);
		// checking if the node is not a leaf, push its leaf onto stack to process 
        if (level >= 0 && !currentNode->isLeaf) {
			//pushing the leafs in reverse order to process then from left to right
            stack[++top] = (StackNode){currentNode->children[2], level + 1};
            stack[++top] = (StackNode){currentNode->children[1], level + 1};
            stack[++top] = (StackNode){currentNode->children[0], level + 1};
        }
    }
}