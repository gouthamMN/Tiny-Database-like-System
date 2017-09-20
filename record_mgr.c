#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

                                                              
char unit=sizeof(char);
int INT=sizeof(int);

BM_PageHandle *ph;	 
BM_BufferPool *bm;	 
RID *rid;
char *Temp;

//DataStructure to keep track of table
typedef struct tableData
{
    int numOfTuples;	        
    int freeSlot;	          
    char *name;               
    BM_PageHandle handlePages;	
    BM_BufferPool poolHandle;	
}tableData;
tableData *ts;

//DataStructure to Scan tuples in table
typedef struct scanTuple
{
    Expr *Condition;        
    RID key;                
    BM_PageHandle handlePages;
} scanTuple;

SM_FileHandle handleFile;
SM_PageHandle handlePage;

#define INCREMENT_PH() handlePage += 4;

						/************** TABLE AND MANAGER ************/

/* function Name: initRecordManager
 * functionality: Initializes and record manager.
 */
RC initRecordManager (void *mgmtData)
{
return RC_OK; //we don't have to initialize the record manager
}

/* function Name: shutdownRecordManager
 * functionality: Shut down the present record manager 
 */
RC shutdownRecordManager ()
{
return RC_OK;//we don't have to shout down the record manager
}

/* function Name: CReateTable
 * functionality: creates new page file for given table and stores the schema in first page.
 */
RC createTable (char *name, Schema *schema)
{          
char pageSize[PAGE_SIZE];
   
int i=0;                                                   
ts=(tableData*)malloc(sizeof(tableData));

bm = &ts->poolHandle;
initBufferPool(bm, name, 50, RS_FIFO, NULL);              

char *handlePages = pageSize;
for(i=0; i<PAGE_SIZE; i++) handlePages[i]=0;

for(i=0;i<=unit;i++)   
{                                                                    
*(int*)handlePages = i;                                              
handlePages += 4;                                           
}                                                           
*(int*)handlePages = 3;                                     
handlePages += 4;                                           
*(int*)handlePages = (int)schema->dataTypes[i];             
handlePages += unit;                                   
*(int*)handlePages = (int) schema->typeLength[i];      
handlePages += unit;                                   

if(name != NULL)        
{  
createPageFile(name);   
openPageFile(name, &handleFile);      
writeBlock(0, &handleFile, pageSize);                         
}
else
{   
return RC_FILE_NOT_FOUND;  
}
return RC_OK;
}

/* function Name: OpenTable
 * functionality: Access the table by accessing the page file using buffer manager .
 */
RC openTable (RM_TableData *rel, char *name)
{
int i=0;
rel->mgmtData = ts;        
ph = &ts->handlePages;   
bm =  &ts->poolHandle;                                
pinPage(bm, ph, -1);                  
handlePage = (char*) ts->handlePages.data;	                             
ts->numOfTuples= *(int*)handlePage; 	  
INCREMENT_PH();
ts->freeSlot= *(int*)handlePage;	      
INCREMENT_PH();
i = *(int*)handlePage;		              
INCREMENT_PH();

Schema *sh;
sh= (Schema*) malloc(sizeof(Schema));
sh->numAttr= i;
sh->attrNames= (char**) malloc(sizeof(char*)*3);
sh->dataTypes= (DataType*) malloc(INT*3); 
sh->typeLength= (int*) malloc(INT*3);  

for(i=0; i<= unit; i++)                                      
sh->attrNames[i]=(char*)malloc(unit+INT); 

for(i=i; i<= (unit+INT); i++) 
sh->dataTypes[i]= *(int*)handlePage;      

rel->schema = sh; 
unpinPage(bm, ph);                     
return RC_OK;    
}

/* function Name: closeTable
 * functionality: closes the table buy shut downing buffer pool .
 */   
RC closeTable (RM_TableData *rel)
{
BM_BufferPool *poolHandle=(BM_BufferPool *)rel->mgmtData;
ts = rel->mgmtData;    
bm = &ts->poolHandle;                          
shutdownBufferPool(bm);	                       
return RC_OK;
}

/* function Name: deleteTable
 * functionality: delets the table by destroying the associated pages of file.
 */   
RC deleteTable (char *name)
{
destroyPageFile(name);	
return RC_OK;
}

/* function Name: getNumTuples
 * functionality: returns the number of tuples of a table.
 */ 
int getNumTuples (RM_TableData *rel)			
{
ts = (tableData*)rel->mgmtData;
return ts->numOfTuples;
}
 
				/*************** HANDLING RECORDS IN A TABLE ********************/
/* function Name: getFreeSlot
 * functionality: returns the available free slot.
 */  
int getFreeSlot(char *v, int size)
{
int i, totalSlots;
totalSlots = ceil(PAGE_SIZE/size); 
for(i=0; i<totalSlots; i++)
{
	if (v[i*size] != '$')
		return i;
}
return -1;
}

/* function Name: insertRecord
 * functionality: will insert new record in available page and slot and will assign that to record parameter.
 */  
RC insertRecord (RM_TableData *rel, Record *record)			    
{
ts = rel->mgmtData;						
rid = &record->id;		                
rid->page = ts->freeSlot;				
bm = &ts->poolHandle;
ph = &ts->handlePages;
int recordSize = getRecordSize(rel->schema);
int i = rid->page, j;

pinPage(bm, ph, i);	   

char *data = ts->handlePages.data;	

rid->slot = getFreeSlot(data, recordSize);			
j = rid->slot;

while(j<0)							   
{                                      
i++;								   
pinPage(bm, ph, i);	 
data = ts->handlePages.data;	
j = getFreeSlot(data, recordSize);	
}

markDirty(bm,ph);			    

data += (j * recordSize);		
*data = '$';					
data++;	 
   rid->slot = j;
rid->page = i;
memmove(data, record->data+1, recordSize);
unpinPage(bm, ph);			   
pinPage(bm, ph, 1);		    
return RC_OK;								    
}

/* function Name: deleteRecord
 * functionality: will delete the record based on the RID parameter that is passed.
 */  
RC deleteRecord (RM_TableData *rel, RID id)			            
{
	bm = &ts->poolHandle;
	ph = &ts->handlePages;
	int tuples, recordSize;
	
	openPageFile(ts->name, &handleFile);		

	int rc = pinPage(bm, ph,id.page);

	if(rc == RC_OK){    
		Temp = ts->handlePages.data; 
	}else{
		return RC_WRITE_FAILED;
	}
	tuples = ts->numOfTuples; 		
	recordSize = getRecordSize(rel->schema); 
	tuples += (id.slot * recordSize); 		
	markDirty(bm, ph); 	 	   
	unpinPage(bm, ph);			

	return RC_OK;								   
}

/* function Name: updateRecord
 * functionality: will update  an  existing record with a new value on the slot and page that has been passed.
 */ 
RC updateRecord (RM_TableData *rel, Record *record)							
{
	bm = &ts->poolHandle;
	ph = &ts->handlePages;
	rid = &record->id;			
	int page, recordSize;
		page = record->id.page;
	
	pinPage(bm, ph, page); 	    
recordSize = getRecordSize(rel->schema);				
Temp = (ts->handlePages.data) + (rid->slot * recordSize); 
Temp += 1;	

memmove(Temp,record->data + 1, recordSize);  		
markDirty(bm, ph); 		   
unpinPage(bm, ph); 		   
return RC_OK;								    
}

/* function Name: getRecord
 * functionality: will get an  existing record value based on RID parameter(id) and assing the value to record plus assign that RID to record->id.
 */ 
RC getRecord (RM_TableData *rel, RID id, Record *record)			
{
	bm = &ts->poolHandle;
	ph = &ts->handlePages;
	int page, recordSize;
		page = id.page;
	
	char *ptr;
		
pinPage(bm, ph, page); 	         
recordSize = getRecordSize(rel->schema);	

ptr = (ts->handlePages.data) + ((id.slot)*recordSize);	
ptr++;
Temp = record->data;				
Temp += 1;							
memmove(Temp,ptr,recordSize - 1);	
record->id = id; 			        
unpinPage(bm, ph); 		   
return RC_OK;									
}

/* function Name: startScan
 * functionality: will scan 30 records at a time.
 */ 
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)			
{

openTable(rel, Temp);		

scanTuple *scanPtr;                                                                \
scanPtr = (scanTuple*) malloc(sizeof(scanTuple)); 

							   
scanPtr->key.page= 1;      				
scanPtr->key.slot= 0;      				
scanPtr->Condition = cond;              
scan->mgmtData = scanPtr;		 

ts = rel->mgmtData; 			
ts->numOfTuples = 30;			
scan->rel= rel;	

return RC_OK;	
}

/* function Name: next
 * functionality: will scan the next records that match the condition.
 */ 
RC next (RM_ScanHandle *scan, Record *record)
{
	scanTuple *scanPtr;                                                                
	scanPtr = (scanTuple*)scan->mgmtData; 
	
	Schema *sptr = scan->rel->schema;
	ts = (tableData*)scan->rel->mgmtData;                              

	Value *valPtr;                                                                     
	valPtr = (Value *) malloc(sizeof(Value));
		
	int recordSize;
	char *ptr;
	bm = &ts->poolHandle;
	ph = &scanPtr->handlePages;	
	
recordSize = getRecordSize(sptr);	
       
int i;					
for(i=0; i <= ts->numOfTuples; i++)			
{
scanPtr->key.slot += 1;				
pinPage(bm, ph, scanPtr->key.page);	
ptr = scanPtr->handlePages.data;				
ptr += (scanPtr->key.slot * recordSize);
record->id.page = scanPtr->key.page; 				
record->id.slot = scanPtr->key.slot;				
Temp = record->data;					
Temp += 1;	
				
memmove(Temp,ptr+1,recordSize-1); 		
evalExpr(record, sptr, scanPtr->Condition, &valPtr);                                   

if((valPtr->v.floatV == TRUE)||(valPtr->v.boolV == TRUE))		
{
unpinPage(bm, ph);		
return RC_OK;						
}
}
unpinPage(bm, ph); 

return RC_RM_NO_MORE_TUPLES;				
}

/* function Name: closeScan
 * functionality: will close a current scan.
 */ 
RC closeScan (RM_ScanHandle *scan)
{
	scanTuple *scanPtr;                                                                
	scanPtr = (scanTuple*)scan->mgmtData; 
	
	bm = &ts->poolHandle;
	ph = &scanPtr->handlePages;	
	
	unpinPage(bm, ph);              

	return RC_OK;
}

/* function Name: getRecordSize
 * functionality: will return the size of a record.
 */ 
int getRecordSize (Schema *schema)
{
	int i, Offset=0;

for(i=0; i<schema->numAttr; i++) 
{                                
if (schema->dataTypes[i] == DT_INT) 
	Offset += 4;                    
else if(schema->dataTypes[i] == DT_STRING) 
	Offset += schema->typeLength[i]; 
}                                    
Offset++;                            
return Offset;                       
}

/* function Name: createSchema
 * functionality: will create and initialise the new schema and returns the pointer to it.
 */ 
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{

Schema *sh=(Schema*)malloc(sizeof(Schema));
sh->numAttr = numAttr;                                      
sh->attrNames = attrNames;                                  
sh->dataTypes =dataTypes;                                   
sh->typeLength = typeLength;                                
sh->keySize = keySize;                                      
sh->keyAttrs = keys;                                        
return sh;                                                  
}

/* function Name: freeSchema
 * functionality: deallocate the memory allocated to a schema.
 */ 
RC freeSchema (Schema *schema)
{
free(schema);                                               
return RC_OK;
}

/* function Name: createRecord
 * functionality: creates a new record 
 */ 
RC createRecord(Record **record, Schema *schema)
{                
int i, recordSize;

for(i=0; i < INT; i++) 					       
{
	switch(*(schema->dataTypes + i))
	{
		case DT_INT:
		case DT_FLOAT: recordSize += INT;	
					break;			
		case DT_BOOL: recordSize += 1;	
					break;
		case DT_STRING: recordSize += (*(schema->typeLength + i));		
					break;
	}
}

for(i=0; i < INT + unit; i++)			      
{
Temp = (char *)malloc(INT + unit);		      
Temp[i]='\0';						       
*record = (Record *)malloc(INT);			
record[0]->data=Temp;					    
}
return RC_OK;								
}

/* function Name: freeRecord
 * functionality: deallocate the memory allocated to record 
 */ 
RC freeRecord (Record *record)
{
free(record);                               
return RC_OK;								
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) 
{
int Offset = 1, i; 				     

DataType *type = schema->dataTypes;
Value *valPtr = (Value *) malloc(sizeof(Value));
Temp = record->data;	

for(i = 0; i < attrNum; i++)
    switch (type[i])
      {
		case DT_STRING:
			Offset += schema->typeLength[i]; 
			break;
      case DT_INT:
			Offset += INT; 
			break;
      case DT_FLOAT:
			Offset += sizeof(float); 
			break;
      case DT_BOOL:
			Offset +=  sizeof(bool); 
			break;
      }

Temp += Offset;		    
if(attrNum == 1) type[attrNum] = 1;			

switch (type[attrNum]){
	case DT_INT:
				memcpy(&valPtr->v.intV,Temp, 4);				
				break;

	case DT_FLOAT:
				memcpy(&valPtr->v.floatV,Temp,4);			     
				break;

	case DT_BOOL:
				memcpy(&valPtr->v.boolV,Temp,1);			     
				break;

	case DT_STRING:
				i = schema->typeLength[attrNum];	    
				valPtr->v.stringV = (char *) malloc(unit);
				strncpy(valPtr->v.stringV, Temp, i);	  
				break;
	default :
				return RC_RM_UNKOWN_DATATYPE;
	
}

valPtr->dt = type[attrNum];
*value = valPtr;						    
return RC_OK;						     
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{

int Offset = 1, i; 	

DataType *type = schema->dataTypes;

for(i = 0; i < attrNum; i++)
    switch (type[i])
      {
		case DT_STRING:
			Offset += schema->typeLength[i]; 
			break;
      case DT_INT:
			Offset += INT; 
			break;
      case DT_FLOAT:
			Offset += sizeof(float); 
			break;
      case DT_BOOL:
			Offset +=  sizeof(bool); 
			break;
      }

Temp = record->data;					    
Temp += Offset;		      

if(type[attrNum] == DT_INT)
	*(int *)Temp = value->v.intV;
else if(type[attrNum] == DT_STRING)
	strncpy(Temp, value->v.stringV, unit+INT);
else 
	return RC_RM_UNKOWN_DATATYPE;
	
return RC_OK;						      
}
