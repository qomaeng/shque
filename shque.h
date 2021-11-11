/*
 * Shared Queue (Multi to Multi process IPC)
 * email : qomaeng@gmail.com
 */

#ifndef __SHQUE_H__20160314MKH
#define __SHQUE_H__20160314MKH

#include <string>
#include <stdexcept>
#include <sys/types.h>
#include <stdint.h>

namespace qoipc
{
    /*
     * l     : just for 64bit operand
     * iNc[0]: head Index of block
     * iNc[1]: dequeue Count
     */
    union Head
    {
        uint64_t l;
        uint32_t iNc[2]; 
    };

    struct ShqueHeader
    {
        union Head readHead;
        union Head writeHead;

        size_t memSize;    /* shared memory size */
        size_t headerSize; /* shared header size */
        size_t blockSize;  /* sizeof(Block) + queueSize */
        size_t queueSize;  /* Block's data size */
        size_t queueMax;

        key_t semKey; /* semaphore key */
        unsigned int blockedReadCount;
        unsigned int blockedWriteCount;
    };

    struct Block 
    {
        uint32_t nextReadableIdx;
        uint32_t nextWritableIdx;
        size_t dataLen;
        char data[0];
    };

    class Shque
    {
    private:
        std::string m_name;
        struct ShqueHeader *m_header;
        Block *m_block;
        /* 
         * used for queue empty/full state 
         * semId's 0: for read queue
         * semId's 1: for write queue
         */
        int m_semId; 

    public:
        Shque(void);
        explicit Shque(const Shque &sq) { throw std::invalid_argument("Copy constructor is not permitted"); }
//		Shque(const Shque& sb) = delete; /* c++11 */
//		Shque(Shque&& sb) = delete;      /* c++11 */
        ~Shque(void);

        void open(std::string name, size_t queueSize = 0, size_t queueMax = 0);

        size_t read (void *dst, size_t size, int blocking = 1); 
        size_t write(void *src, size_t size, int blocking = 1);

        size_t getQueueSize(void) { return m_header->queueSize; }
        size_t getQueueMax(void)  { return m_header->queueMax;  }
        
        Shque& operator=(const Shque&) { throw std::invalid_argument("Copy assignment is not permitted"); }
//		Shque& operator=(const Shque&) = delete; /* c++11 */
//		Shque& operator=(Shque&&)      = delete; /* c++11 */

    private:
        int queueReadableBlock(uint32_t index);
        int queueWritableBlock(uint32_t index);
        uint32_t dequeReadableBlock(int blocking = 1);
        uint32_t dequeWritableBlock(int blocking = 1);

        int semInit(key_t key);
        int waitReadable(void);
        int waitWritable(void);
        int awakeBlockedRead(void);
        int awakeBlockedWrite(void);
    };
}

#endif /* __SHQUE_H__20160314MKH */
