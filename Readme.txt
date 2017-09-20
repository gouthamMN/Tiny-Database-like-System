
This is In-Memory tiny database-like System which performs storage, buffer and record management using LRU-K and CLOCK mechanism over FIFO.

Programming Language: C
Tool: Putty
Environment: Linux

Index
1. Instruction to Run the Code
2. Functionality Used Description 

************************************************************************
1. Instruction to Run the Code
*************************************************************************
		1) Navigate to the directory of the assignment contents on the Linux/Unix tool(Ubuntu)
		2) Type: 
				make -f Makefile.mk
		3) ./TestAssn3
		4) Use below command to delete all output files in directory after step 3
				make -f Makefile.mk clean



**************************************************************************************************
 2. Functional description.
*************************************************************************************************/
	Function : createTable
	-----------------------

A table is created and the bufferpool is initialized.
page size is set with PAGE_SIZE(4096).
A check is made to see whether a value is assigned to the bufferpool, if not value is assigned.
A check is made to see whether page is created or not, if craeted we try opening to and write in it.



	Function : openTable
	---------------------

The page that is opened from the bufferpool is pinned.
allocate the memory for attribute and check if the value is less than number of attributes.
allocate the memory for data type and check if the value is less than number of attributes and size of integer.
The page that is opened in unpinned.


	Function : closeTable
	-----------------------------
A sucess message is displayed and the table is closed.
.


	Function : deleteTable
	------------------------------
The destroy page function is called to destroy the table.


	
	Function : getNumTuples
	-------------------------
To get the number of tuples.


	Function : insertRecord
	-------------------------
In order to insert a record where the slot is free tha page is pinned.
Check whether key slot is les than zero, pagesize is incremented and the value to the record is added.
Since the record was recently changed it is marked as dirty	and the content is moved to memeory.
the record modified recently is pinned.



	Function : deleteRecord
	---------------------
the page is opened and checked if its pinned or not.
the content is moved and marked as dirty and unppinned.
	


	Function : updateRecord
	----------------------------
Where the update has to be done get its record id.
update to location by pinning it.
the content is moved to memory, made dirty and unpin it.



	Function : getRecord
	---------------------------
from where the record has to be fetched pin that page.
in pointer value the record value is found.
the content is moved to memory and unpinned.


	Function : startScan
	-----------------------
	Scans the particular record.


	Function : next
	------------------------

verify if  values are scanned.
scanned values are moved  to the memory.
unpin page by checking the boolean is true or not.
all the scanned pages are unpinned and the tuples are returned.



	Function: closeScan
	---------------------

if all the record are scanned the data is made null and freed.
if failed unpin page and return.
	


	Function : getRecordSize
	-------------------------------
Based oon the number of attributes the data types are returned.
the attribute offset is returned based on integer or string.
	



	Function : createRecord
	-----------------------------

verify if the temp value is less than the size of the integer.
for integer datatype the value of the size f attribute is assigned with the tenp value of the record size.
on creation of record a succes message is returned.


	Function : freeRecord
	----------------------
free function is called to free the function



	Function : getAttr
	------------------
change the datatype value according to the value stored to temp value from the pointer value.
the stored value is returned.
	



	Function : setAttr
	------------------

change the datatype value according to the value stored to temp value from the pointer value.
the values are set.

