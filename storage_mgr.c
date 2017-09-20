#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

FILE *fp;

void initStorageManager (){
 printf("Storage manager initialised\n\n");
	//No need to implement this as we don't have any global data structure to initialise
	}

/*this method used to Create file if not exists
*	fileName- is the parameter that points to file 
*/
RC createPageFile(char *fileName){
	//FILE *fp;
	
	if(access(fileName, F_OK) == 0){
		printf("File, %s already exists \n\n", fileName);
		return RC_OK;
	}
	
	fp=fopen(fileName,"w+");
	
	if(fp == NULL){
		printf("File ceation failed\n\n");
		return RC_FILE_CREATION_FAILED;
	}
	else{
		printf("File, %s has been created\n\n", fileName);
		
		char *input = (char*) malloc(PAGE_SIZE);
		int i=0;
		while(i<PAGE_SIZE){
			input[i]='\0';
			i++;
		}
		
		if(fwrite(input,sizeof(char),PAGE_SIZE,fp) != PAGE_SIZE){
			return RC_WRITE_TO_OUTPUTSTREAM_FAILED;
		}
		
		free(input);
		if(fclose(fp) != 0) 
			return RC_FILE_NOT_CLOSED;
		else
			return RC_OK;
	}
}

/*this method used open file to read or write
filename- points to file 
fHandle- points to file handler structure*/
RC openPageFile (char *fileName, SM_FileHandle *fHandle){
 
  if(fileName != NULL){
    fp =fopen(fileName,"r+");
  }
  else{
    printf("File, %s do not exists \n\n", fileName);
		return RC_FILE_NOT_FOUND;
  }
 
 if(fp!=NULL){
	 
		struct stat statPointer;
		
		//printf("\n\ntotal pages %i\n",fHandle->totalNumPages);
		//count the total number of page in a file
		if(stat(fileName, &statPointer) != -1 ){
			int numberOfPages;
			numberOfPages = (statPointer.st_size)/PAGE_SIZE;
			
			if((statPointer.st_size)%PAGE_SIZE > 0)
				fHandle->totalNumPages = numberOfPages+1;
			else
				fHandle->totalNumPages = numberOfPages;
		}
		fHandle->totalNumPages =1;
		fHandle->fileName=fileName;
		fHandle->curPagePos=0;
		fHandle->mgmtInfo=fp;
		fclose(fp);
		//printf("total pages after %i",fHandle->totalNumPages);	
		return RC_OK;

  }
  else{
    printf("Error:File Not Found");
    return RC_FILE_NOT_FOUND;
  }
}

/* This method closes the file and frees the memory
fHandle- is the pointer to file handler structure that has the details of file to be closed*/
RC closePageFile(SM_FileHandle *fHandle){
	//FILE *fp=NULL;
	
	if(fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	else
		fp=fHandle->mgmtInfo;
	
	if(fclose(fp)==0){
		printf("file, %s has been closed \n\n",fHandle->fileName);
		return RC_OK;
	}else{
		return RC_FILE_NOT_CLOSED;
	}
}

/*This method deletes the file from memory
fileName- is the pointer to file that need to be closed*/
RC destroyPageFile(char *fileName){
	if(remove(fileName)==0)
		return RC_OK;
	else
		return RC_FILE_NOT_FOUND;
}

/*This method reads the block from file and loads the data into memory
pageNum- this specify which block need to be read 
memPage- this is the pointer to memory where data will be loaded from file*/
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    fp = fopen(fHandle->fileName,"r+");
	
	if(fHandle == NULL) 
			return RC_FILE_HANDLE_NOT_INIT;
		
	/*if(!fp)	//check if the page is closed, if so, then open
		{
			fp= fopen(fHandle->fileName,"r");
			//fHandle->mgmtInfo=fp;
		}else
			return RC_FILE_READ_FAILED;
	*/
	
	fseek(fp,(pageNum*PAGE_SIZE),SEEK_SET);

 fread(memPage,sizeof(char),PAGE_SIZE,fp);
          fHandle->curPagePos=ftell(fp);
          fclose(fp);
          return RC_OK;
		
}

/* returns the current page possition in a file*/
int getBlockPos(SM_FileHandle *fHandle){
			return fHandle->curPagePos;
}		

/*this method reads the first block/page in a file */
  RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
		return readBlock(0,fHandle,memPage);	//call readBlock page by passing 0 as the page Number value
}

/*this method reads the previous block/page of a file with respect to curPagePos*/
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	FILE *fp;
	int pageNum;
	
	fp=fHandle->mgmtInfo;
	pageNum=(fHandle->curPagePos)-1;
	
	if(fp==NULL || pageNum<0)
		return RC_READ_NON_EXISTING_PAGE;
	else
		return readBlock(pageNum, fHandle, memPage);
}

/*this method reads the current block/page of a file*/
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	FILE *fp;
	int pageNum;
	
	fp=fHandle->mgmtInfo;
	pageNum=(fHandle->curPagePos);
	
	if(fp==NULL || pageNum<0 || pageNum>fHandle->totalNumPages)
		return RC_READ_NON_EXISTING_PAGE;
	else
		return readBlock(pageNum, fHandle, memPage);
}

/*this method reads the next block/page of a file with respect to curPagePos*/
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	FILE *fp;
	int pageNum;
	
	fp=fHandle->mgmtInfo;
	pageNum=(fHandle->curPagePos)+1;
	
	if(fp==NULL || pageNum<0 || pageNum>fHandle->totalNumPages)
		return RC_READ_NON_EXISTING_PAGE;
	else
		return readBlock(pageNum, fHandle, memPage);
}

/*this method reads the last block/page in a file*/
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	int LastPage=(fHandle->totalNumPages)-1;
	return readBlock(LastPage,fHandle,memPage);
}		

/*This method writes a data from memory to page file that is present on disk and page number is passed in the parameter*/
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
		FILE *fp=fopen(fHandle->fileName,"r+");
		fHandle->mgmtInfo=fp;
	/*	
		if(pageNum<0 || pageNum>fHandle->totalNumPages || fp==NULL){
			return RC_WRITE_FAILED;
		}
		*/
		if(!fp)	//check if the page is closed, if so, then open
		{
			fp= fopen(fHandle->fileName,"r+");
			fHandle->mgmtInfo=fp;
		}
		
		if(fseek(fp,PAGE_SIZE*(pageNum),SEEK_SET)!=0)
		{
			return RC_FILE_OFFSET_FAILED;
		}
		else{
			int rc = fwrite(memPage,sizeof(char),PAGE_SIZE,fp);
			if(rc != PAGE_SIZE){
				return RC_WRITE_FAILED;
			}else{
				fclose(fp);
				fHandle->curPagePos=ftell(fp);
				return RC_OK;
			}
		}
}

/*This method writes a data from memory to current page file*/
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
		
		int pageNum=fHandle->curPagePos;
		return writeBlock(pageNum, fHandle, memPage);
}

/*This method appends a new page at the end of the file*/
RC appendEmptyBlock(SM_FileHandle *fHandle){
	
		FILE *fp=fopen(fHandle->fileName,"a"); //open the file in append mode
		
		char *input = (char*) malloc(PAGE_SIZE);
		int i=0;
		while(i<PAGE_SIZE){
			input[i]='\0';
			i++;
		}
		
		if(fwrite(input,sizeof(char),PAGE_SIZE,fp) != PAGE_SIZE){
			return RC_WRITE_FAILED;
		}
		
		free(input);
		fHandle->totalNumPages+1; //increase the total number of page count by 1
		if(fclose(fp) != 0) 
			return RC_FILE_NOT_CLOSED;
		else
			return RC_OK;
}

/*This method ensures the number of pages in the file*/
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle){
	FILE *fp;
	
	if(fHandle==NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	
	fp=fHandle->mgmtInfo;
	if(!fp){
		fp = fopen(fHandle->fileName,"r+");
	}
	
	fseek(fp, 0L, SEEK_END);
	int pagesInFile = ftell(fp)/PAGE_SIZE;
	
	if(ftell(fp)%PAGE_SIZE > 0){
		pagesInFile=pagesInFile+1;
	}
	
	if(pagesInFile < numberOfPages){
		
		int pagesToAdd = numberOfPages - pagesInFile;
		int totalSize = (pagesToAdd)*PAGE_SIZE;
		int i=0;
		
		while(i<totalSize){
			fprintf(fp, "%c", '\0');
			i++;
		}
		
		fHandle->totalNumPages = (fHandle->totalNumPages)+pagesToAdd;
		return RC_OK;
	}
}

