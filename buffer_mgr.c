//Including all the neccessary header files 
#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

//Struct for every frame page
typedef struct FrameDetail
{
  SM_PageHandle data;
  int pageNumber;
  int isDirty;
  int C_count;	
  int f_count;
  int No_Replace;
}FrameDetail;


int count = 0;
SM_FileHandle fh;
FrameDetail *fPtr;

void getPageFromFile(BM_BufferPool *const bm, int i, BM_PageHandle *const page, const PageNumber pageNum);
void * allocateMemory(int size,int type);

/*
This function allocates memory based on the type parameter
*/
void * allocateMemory(size, type){
  void *ptr=NULL;
  switch(type){
	  case 1:  ptr= (char *)malloc(sizeof(FrameDetail)*size);
		break;
  
	  case 2 : ptr= (char *)malloc(sizeof(int)*size);
			break;
  
	  case 3: ptr= (char *)malloc(sizeof(bool)*size);
  			break;
			
	  case 4: ptr= (int *)malloc(sizeof(int)*size);
			break;
			
	  default:
			break;
  }
  return ptr;
}

/*********************************************
  this method initialize the bufferpool and framedetail struct
********************************************/
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData)
{
	FrameDetail *frame = NULL;
	
	if(bm == NULL || numPages <=0)	
		return RC_INVALID_BM;
	
	if(pageFileName == NULL) 
		return RC_FILE_NOT_FOUND;
	
	if(strategy < 0) 
		return RC_UNKNOWN_STRATEGY;
	
   frame = allocateMemory(numPages, 1);
   
  if(frame != NULL){
  
  bm->pageFile = (char *)pageFileName;
  bm->numPages = numPages;
  bm->strategy = strategy;
  bm->mgmtData = frame;
 
 int i=0;
  
  for(i=0; i<numPages; i++ ){
    frame[i].data = NULL;
    frame[i].pageNumber = -1;
    frame[i].C_count = frame[i].f_count = frame[i].isDirty = 0;
  }
	bm->readcount = 0;
  bm->writecount = 0;
  return RC_OK;
  }else{
	  return RC_MEMORY_ERROR;
  }
 
}

/*********************************************
 this method shutdown the buffer pool 
  that is passed as parameter
*********************************************/
RC shutdownBufferPool(BM_BufferPool *const bm)
{
  int i=0;
	FrameDetail *fd = bm->mgmtData;
  if(bm == NULL)
	return RC_NULL_ERROR;
  
  if(forceFlushPool(bm) == RC_OK){
  
  while(i< bm->numPages)
  {
    if(fd[i].C_count != 0)
    return RC_PINNEN_PAGES_IN_BUFFER;

	i++;
  }
  fd = NULL;
  
  return RC_OK;
  }else{
	    return RC_WRITE_ERROR;
  }
  }
  

/*****************************************
  this method write all dirty page to the disk
*******************************************/
RC forceFlushPool(BM_BufferPool *const bm)
{
   if(bm == NULL)
	    return RC_NULL_ERROR;

  int i=0;
	FrameDetail *fd = bm->mgmtData;
	
  while(i<bm -> numPages)
  {
    if(fd[i].isDirty == 1 && fd[i].C_count == 0)
    {
      writeBlock (fd[i].pageNumber, &fh, fd[i].data);
      bm->writecount++;
      fd[i].isDirty = 0;
    }
	i++;
  }
  
  return RC_OK;

}

/************************************************************
 this method mark the page dirty and return the pageHandle
*************************************************************/
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	if(bm == NULL || page == NULL)
		return RC_NULL_ERROR;
	
  int i=0;
  int numPages = bm->numPages;
  FrameDetail *fd = bm->mgmtData;
  
  while(i<bm -> numPages)
    {
      if(fd[i].pageNumber == page->pageNum)
      {
        fd[i].isDirty = 1;
        return RC_OK;
      }
	  i++;
    }
  return RC_OK;

}

/************************************************
  this method unpin the page from bufferpool and decrease the C_count
************************************************/
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
  if(bm == NULL || page == NULL)
		return RC_NULL_ERROR;
	
  int i=0;
  int numPages = bm->numPages;
  FrameDetail *fd = bm->mgmtData;
  
  while(i<bm -> numPages)
    {
      if(fd[i].pageNumber == page->pageNum)
      {
        fd[i].C_count = fd[i].C_count-1;
        break;
      }
	  i++;
    }
  return RC_OK;

}

/***************************************
 this method write page back to disk from bufferpool
****************************************/
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
  if(bm == NULL || page == NULL)
  {
	    return RC_NULL_ERROR;
  }
  
  if(writeDirtyPage(bm,page)!=RC_OK)
  {
	return RC_WRITE_ERROR;
	}
	else{
		return RC_OK;
	}

}

/*********************************************************
 this method write dirty page back to disk from bufferpool
**********************************************************/
RC writeDirtyPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if(bm !=NULL && page != NULL){
     
     int i;
     for(i=0;i<bm -> numPages;i++)
    {
    if(((FrameDetail  *)bm->mgmtData)[i].pageNumber == page->pageNum)
    {
     ensureCapacity(((FrameDetail  *)bm->mgmtData)[i].pageNumber, &fh);
     writeBlock (((FrameDetail  *)bm->mgmtData)[i].pageNumber, &fh, ((FrameDetail  *)bm->mgmtData)[i].data);
     bm->writecount++;
     ((FrameDetail  *)bm->mgmtData)[i].isDirty = 0;
      
     return RC_OK;
    }
  }
  
}
else{
  return RC_NULL_ERROR;
}
}

/***************************************
 First In First out Stratergy
****************************************/
void FIFO(BM_BufferPool *const bm, FrameDetail *fd)
{
  int i;
  int pages = bm -> numPages;
  int previous = bm->readcount%pages;
  fPtr = (FrameDetail *) bm->mgmtData;
  
  for(i=0; i<pages; i++){
    if(fPtr[previous].C_count == 0)
	  {
      if(fPtr[previous].isDirty == 1){
        openPageFile (bm->pageFile, &fh);
        writeBlock (fPtr[previous].pageNumber, &fh, fPtr[previous].data);
        bm -> writecount++;
	    }
    	 fPtr[previous].data = fd->data;
    	 fPtr[previous].pageNumber = fd->pageNumber;
    	 fPtr[previous].isDirty = fd->isDirty;
    	 fPtr[previous].C_count = fd->C_count;
    	 break;
	  }
	  else
	  {
       previous++;
	     if(previous%pages == 0)
       previous=0;
	  }
  }
}


/***************************************
 Least Recently Used Stratergy
****************************************/
void LRU(BM_BufferPool *const bm, FrameDetail *fd)
{
  fPtr=(FrameDetail *) bm->mgmtData;
  int i=0;
  int pages = bm->numPages;
  int previous;
  int least;
  for(i=0; i<pages; i++)
  {
    if(fPtr[i].C_count == 0)
    {
      previous= i;
      least = fPtr[i].f_count;
      break;
    }
  }
   
    for(i=previous+1; i<pages; i++)
    {
      if(fPtr[i].f_count < least)
      {
        previous = i;
        least = fPtr[i].f_count;
      }
    }
    if(fPtr[previous].isDirty == 1){
      openPageFile (bm->pageFile, &fh);
      writeBlock (fPtr[previous].pageNumber, &fh, fPtr[previous].data);
      bm -> writecount++;
    }
    fPtr[previous].data = fd->data;
    fPtr[previous].pageNumber = fd->pageNumber;
    fPtr[previous].isDirty = fd->isDirty;
    fPtr[previous].C_count = fd->C_count;
    fPtr[previous].f_count = fd->f_count;
}

/************************************************
 this method pins the page with different strategy(FIFO/LRU)
************************************************/
  RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	if(bm == NULL || page == NULL)
		return RC_PIN_PAGE_PARAMETER_ERROR;
	
  if(((FrameDetail *)bm->mgmtData)[0].pageNumber == -1)
  {
	  getPageFromFile(bm, 0, page, pageNum);
      bm->readcount = 0;
      count = 0;
      ((FrameDetail *)bm->mgmtData)[0].f_count = count;
   return RC_OK;
  }
   else
   {
      int i=0;
      int buffer_check = 0;
      while(i<bm -> numPages)
      {
        if(((FrameDetail *)bm->mgmtData)[i].pageNumber != -1)
        {
	         if(((FrameDetail *)bm->mgmtData)[i].pageNumber == pageNum)
	         {
              ((FrameDetail *)bm->mgmtData)[i].C_count++;
            	buffer_check = 1;
            	count++;
	            if(bm->strategy == RS_LRU)
              ((FrameDetail *)bm->mgmtData)[i].f_count = count;
	            page->pageNum = pageNum;
	            page->data = ((FrameDetail *)bm->mgmtData)[i].data;
              break;
	         }
        }
        else
        {
           getPageFromFile(bm, i, page, pageNum);
            bm->readcount++;
            count++;
      	    if(bm->strategy == RS_LRU)
        	  ((FrameDetail *)bm->mgmtData)[i].f_count = count;
        	  buffer_check = 1;
        	  break;
        }
        i++;
      }
	  
	   if(buffer_check == 0) callPageReplacement(bm, page, pageNum);
	   
	   return RC_OK;
	}
}

RC callPageReplacement(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	FrameDetail *frame;
	frame = (FrameDetail *)malloc(sizeof(FrameDetail));
	frame->data = (SM_PageHandle) malloc(PAGE_SIZE);
    openPageFile (bm->pageFile, &fh);
    readBlock(pageNum, &fh, frame->data);
    frame->pageNumber = pageNum;
    frame->isDirty = 0;
    frame->C_count = 1;
    frame->No_Replace = 0;
    page->pageNum = pageNum;
    page->data = frame->data;
    bm->readcount++;
    count++;
    int replacementAlgo = bm->strategy;
	
	if(replacementAlgo == RS_FIFO){
        FIFO(bm,frame);
    }
    else if (replacementAlgo == RS_LRU){
		    frame->f_count = count;
			LRU(bm,frame);
    }else{
        return RC_NULL_ERROR;
    }
}

void getPageFromFile(BM_BufferPool *const bm, int i, BM_PageHandle *const page, const PageNumber pageNum){
fPtr = (FrameDetail *)bm->mgmtData;

	openPageFile (bm->pageFile, &fh);
   fPtr[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
   readBlock(pageNum, &fh, fPtr[i].data);
   fPtr[i].pageNumber = pageNum;
   fPtr[i].C_count=1;
   fPtr[i].No_Replace = 0;
   page->pageNum = pageNum;
   page->data = fPtr[i].data;
   }

/************************************************
  this method returns the frame contents
************************************************/
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
  int i;
  int pages = bm->numPages;
  
  if(bm == NULL){
	  return (int *)RC_INVALID_BM;
  }
  
  PageNumber *pageNo = allocateMemory(pages, 2);
  
  for(i=0;i<pages;i++)
  {
  pageNo[i] = ((FrameDetail  *)bm->mgmtData)[i].pageNumber;
  }
 
 return pageNo;
}

/************************************************
  this method returns a boolean array of the dirty page flags
************************************************/
bool *getDirtyFlags (BM_BufferPool *const bm)
{
   if(bm == NULL){
	  return (bool *)RC_INVALID_BM;
  }
   
   int pages = bm->numPages;
   int i;
	bool *dirtyPage;
   
   dirtyPage = allocateMemory(pages, 3);
   
  for(i=0;i<pages;i++)
  {
    if(((FrameDetail  *)bm->mgmtData)[i].isDirty == 1)
    {
      dirtyPage[i]= 1;
    }
    else
    {
      dirtyPage[i]=0;
    }
  }
  return dirtyPage;
}

/************************************************
 this method returns fix count of all the page
************************************************/
int *getFixCounts (BM_BufferPool *const bm)
{
	
  if(bm == NULL)
  {
		return (int *)RC_INVALID_BM;
	}

	int i=0;
	int pages = bm->numPages;
	int *fixCount;
	fixCount = allocateMemory(pages, 4);

  
  while(i<pages)
  {
  fixCount[i] = ((FrameDetail  *)bm->mgmtData)[i].C_count;
  i++;
  }
  
  return fixCount;

}

/************************************************
  this method return number of time file read
************************************************/
int getNumReadIO (BM_BufferPool *const bm)
{
  if(bm == NULL)
  {
	  return RC_INVALID_BM;
  }
  
 
  int readCount = bm->readcount+1;
  
  if(readCount < 0)
  {
  return RC_READ_COUNT_ERROR;
}
else{
  return readCount;
}
}


/************************************************
  this method return number of time file write
************************************************/
int getNumWriteIO (BM_BufferPool *const bm)
{
	if(bm == NULL)
  {
	  return RC_INVALID_BM;
  }
  
  int writeCount = bm->writecount;
  
  if(writeCount < 0)
  {
  return RC_WRITE_COUNT_ERROR;
}
else{
  return writeCount;
}
}

