/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs) {
    bufDescTable = new BufDesc[bufs];

    for (FrameId i = 0; i < bufs; i++) {
        bufDescTable[i].frameNo = i;
        bufDescTable[i].valid = false;
    }

    bufPool = new Page[bufs];

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr() {
    for (FrameId i = 0; i < numBufs; i++) {
        if (bufDescTable[i].valid) {
            BufMgr::flushFile(bufDescTable[i].file);
        }
    }

    delete[] bufDescTable;
    delete[] bufPool;
    delete hashTable;
}

void BufMgr::advanceClock() {
    clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId& frame) {
    advanceClock();

    uint8_t bufPinned = 0;
    while (bufDescTable[clockHand].valid) {
        advanceClock();
        if (bufDescTable[clockHand].refbit) {
            bufDescTable[clockHand].refbit = false;
        } else {
            if (bufDescTable[clockHand].pinCnt == 0) {
                if (bufDescTable[clockHand].dirty) {
                    bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                }
                bufDescTable[clockHand].valid = false;
            } else {
                bufPinned++;
            }
        }
        if (bufPinned == numBufs) {
            throw BufferExceededException();
        }
    }

    frame = clockHand;
}

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
    FrameId frame;

    try {
        hashTable->lookup(file, pageNo, frame);
        bufDescTable[frame].refbit = true;
        bufDescTable[frame].pinCnt++;
    } catch (const HashNotFoundException& e) {
        allocBuf(frame);
        bufPool[frame] = file->readPage(pageNo);
        hashTable->insert(file, pageNo, frame);
        bufDescTable[frame].Set(file, pageNo);
    }
    page = &bufPool[frame];
}

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {
    FrameId frame;
    try {
        hashTable->lookup(file, pageNo, frame);
        if (bufDescTable[frame].pinCnt == 0) {
            throw PageNotPinnedException(file->filename(), pageNo, frame);
        }
        bufDescTable[frame].pinCnt--;
        bufDescTable[frame].dirty |= dirty;
    } catch (const HashNotFoundException& e) {
    }
}

void BufMgr::flushFile(const File* file) {
    for (FrameId frame = 0; frame < numBufs; frame++) {
        if (bufDescTable[frame].file == file) {
            if (!bufDescTable[frame].valid) {
                throw BadBufferException(frame, bufDescTable[frame].dirty, bufDescTable[frame].valid, bufDescTable[frame].refbit);
            }
            if (bufDescTable[frame].pinCnt > 0) {
                throw PagePinnedException(file->filename(), bufDescTable[frame].pageNo, frame);
            }
            if (bufDescTable[frame].dirty) {
                bufDescTable[frame].file->writePage(bufPool[frame]);
                bufDescTable[frame].dirty = false;
            }
            hashTable->remove(file, bufDescTable[frame].pageNo);
            bufDescTable[frame].Clear();
        }
    }
}

void BufMgr::allocPage(File* file, PageId& pageNo, Page*& page) {
    Page page_tmp = file->allocatePage();
    pageNo = page->page_number();

    FrameId frame;
    allocBuf(frame);
    hashTable->insert(file, pageNo, frame);
    bufDescTable[frame].Set(file, pageNo);
    bufPool[frame] = page_tmp;
    page = &bufPool[frame];
}

void BufMgr::disposePage(File* file, const PageId PageNo) {
    FrameId frame;
    try {
        hashTable->lookup(file, PageNo, frame);
        bufDescTable[frame].Clear();
    } catch (const HashNotFoundException& e) {
    }

    hashTable->remove(file, PageNo);
    file->deletePage(PageNo);
}

void BufMgr::printSelf(void) {
    BufDesc* tmpbuf;
    int validFrames = 0;

    for (std::uint32_t i = 0; i < numBufs; i++) {
        tmpbuf = &(bufDescTable[i]);
        std::cout << "FrameNo:" << i << " ";
        tmpbuf->Print();

        if (tmpbuf->valid == true)
            validFrames++;
    }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
