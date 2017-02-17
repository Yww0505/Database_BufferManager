/**
 * @author Wan Yang  ID: 9073802705  CS login: wany  Email: wyang74@wisc.edu
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */
 
/** 
 * @brief This file is the detailed implementation of the buffer manager class
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 
/**
 * Constructor of the buffer manager class
 * @para bufs the capacity of the manager
 */
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (std::uint32_t i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/**
 * Destructor of buffer manager, delete corresponding fields
 *
 */
BufMgr::~BufMgr() {
	for(std::uint32_t i = 0; i < numBufs; i++) {
		if(bufDescTable[i].dirty) {
			bufDescTable[i].file->writePage(bufPool[i]);
		}
	}
	delete hashTable;
	delete[] bufPool;
	delete[] bufDescTable;
}

/**
 * advance the current clockHand(index of frame) by one position
 * if it's out of the bound, reset it to the first position(like the clock)
 */
void BufMgr::advanceClock()
{
		clockHand++;
		clockHand = (clockHand) % (numBufs);
}

/**
 * allocate one free frame in buffer pool according to the clock algorithm 
 * @para frame the allocated frame to be returned
 * @throw BufferExceededException if all frame pages are currently pinned 
 */
void BufMgr::allocBuf(FrameId & frame) 
{
	// the maximum times to search is twice of the buffer size
	// which is, only the farest frame is unpinned and refbit is true at the beginning
	std::uint32_t maxBound = 2 * numBufs;
	advanceClock();
	while(maxBound > 0) {
		if(!bufDescTable[clockHand].valid) {
			break;
		}
		if(bufDescTable[clockHand].refbit) {
			bufDescTable[clockHand].refbit = false;
			advanceClock();
			maxBound--;
			continue;
		} else if (bufDescTable[clockHand].pinCnt > 0) {
			advanceClock();
			maxBound--;
			continue;
		}
		if(bufDescTable[clockHand].dirty) {
			bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
		}
		hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
		bufDescTable[clockHand].Clear();
		break;
	}
	if(maxBound == 0) {
		throw BufferExceededException();
	}
	frame = clockHand;
}

/**
 * read page from buffer pool, if already buffered, just return, otherwise allocate a new frame
 * @para file the file to be readed
 * @para pageNo the page number in the specific file
 * @para page the pointer to page to be returned
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	if(hashTable->lookup(file, pageNo, frameNo)) {
		bufDescTable[frameNo].pinCnt++;
		bufDescTable[frameNo].refbit = true;
	} else {
		allocBuf(frameNo);
		bufPool[frameNo] = file->readPage(pageNo);
		bufDescTable[frameNo].Set(file, pageNo);
		hashTable->insert(file, pageNo, frameNo);
	}
	page = &bufPool[frameNo];
}

/**
 * @file the file including the page
 * @pageNo the page number to be unpinned
 * @dirty whether the page is modified or not
 * @throw PageNotPinnedException if the current page is not pinned 
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
		FrameId frameNo;
		if(hashTable->lookup(file, pageNo, frameNo)) {
			if(bufDescTable[frameNo].pinCnt <= 0) {
				throw PageNotPinnedException(file->filename(), pageNo, frameNo);
			} else {
				bufDescTable[frameNo].pinCnt--;
			}
			if(dirty) {
				bufDescTable[frameNo].dirty = true;
			}
		}
}

/**
 * flush all the pages associated with the file back to memory and clean the buffer records
 * @para file, the file to be checked and flushed back
 */
void BufMgr::flushFile(const File* file) 
{
	for(std::uint32_t i = 0; i < numBufs; i++) {
		BufDesc &curr = bufDescTable[i];
		// if find a entry is invalid but other fields are valid
		if(!curr.valid && (curr.pinCnt != 0 || curr.file != NULL || curr.pageNo != Page::INVALID_NUMBER || curr.dirty || curr.refbit)) {
			throw BadBufferException(i, curr.dirty, false, curr.refbit);
		}
		if(!curr.valid || curr.file != file) {
			continue;
		}
		if(curr.pinCnt > 0) {
			throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
		}
		if(curr.dirty) {
			curr.file->writePage(bufPool[i]);
		}
		hashTable->remove(curr.file, curr.pageNo);
		curr.Clear();	
	}
}

/**
 * allocate new page in the buffer pool and update its record in the hashtable and bufDescTable
 * @para file the file to be readed and load the page
 * @para pageNo the page id to specify the page in the file 
 * @para page the pointer to the buffered page to be returned 
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	Page new_pg;
	FrameId frame;
	new_pg = file->allocatePage();
	allocBuf(frame);
	hashTable->insert(file, new_pg.page_number(), frame);
	bufDescTable[frame].Set(file, new_pg.page_number());
	bufPool[frame] = new_pg;
	pageNo = new_pg.page_number();
	page = &bufPool[frame];
}

/**
 * delete the specific page from the file, and if the page is previously buffered, clean all related record
 * @para file the file to be handled
 * @para PageNo the page id that indicating which page should be deleted
 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId frameId;
    file->deletePage(PageNo);
    if(hashTable->lookup(file, PageNo, frameId)) {
    	hashTable->remove(file, PageNo);
    	bufDescTable[frameId].Clear();
    }
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	    tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
