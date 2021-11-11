/*
 * Shared Queue (Multi to Multi process IPC)
 * email : qomaeng@gmail.com
 */

// TODO: Change 'strerror()' to thread safe function
// TODO: consider Huge page tlb of 'mmap()'
// TODO: add processing check name of shared queue

/*
 * semtimedop 
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/*
 * nanosleep
 */
#if     _POSIX_C_SOURCE < 199309L 
#undef  _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 199309L
#endif

/* 
 * ftruncate 
 */
#if     _XOPEN_SOURCE < 500
#undef  _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

/* 
 * ftruncate 
 * TODO: since glibc 2.3.5
 */
#if     _POSIX_C_SOURCE < 200112L
#undef  _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200112L
#endif

/* 
 * ftruncate 
 * TODO: glibc versions <= 2.19
 * #ifndef _BSD_SOURCE
 * #define _BSD_SOURCE
 * #endif
 */

#include <stdexcept>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>
#include "shque.h"

#define SHQ_SEM_PERM (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)

static key_t getKey(std::string fileName);

using namespace qoipc;

extern "C" 
int c_open(const char *name, int flag, mode_t omode)
{
    return open(name, flag, omode);
}

Shque::Shque(void)
{
    m_header = NULL;
    m_block = NULL;
    m_semId = -1;

    return;
}

Shque::~Shque(void)
{
    if (m_header != NULL)
        if (munmap(m_header, m_header->memSize) == -1)
            ; // TODO: handle error

    m_header = NULL;
    m_block = NULL;
    m_semId = -1;
    m_name = "";
}

void
Shque::open(std::string name, size_t queueSize, size_t queueMax) 
{
    int shmfd;
    int savedErrno;

    m_name = name;

    if (m_name.length() == 0)
        throw std::invalid_argument("Valid name is required");
    
    if (m_name[0] != '/')
        m_name.insert(0, "/");
    
    if (queueSize != 0 && queueMax != 0)
    {
shm_create:
        /*
         * queue index's MSB is used readable block index in read queue
         * and in write queue -1 is represent queue full state
         */
        if (queueMax > 0x7fffffff)
            throw std::invalid_argument("too much queue max");
    
        ///////////////////////////////////////////////////
        //             create shared memory              //
        ///////////////////////////////////////////////////
        shmfd = shm_open(m_name.c_str(), O_CREAT | O_EXCL | O_RDWR,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    
        if (shmfd == -1)
        {
            if (errno == EEXIST)
                goto shm_get;
    
            throw std::runtime_error((std::string) "shm_open-" + strerror(errno));
        }
    
        size_t blockSize = offsetof(Block, data) + queueSize;
        blockSize += (__alignof__(Block) - blockSize % __alignof__(Block)) % __alignof__(Block);

        size_t headerSize = sizeof(ShqueHeader);
        headerSize += (__alignof__(Block) - headerSize % __alignof__(Block)) % __alignof__(Block);

        off_t memSize = headerSize + queueMax * blockSize;
        
        if (ftruncate(shmfd, memSize) == -1)
        {
            savedErrno = errno;
            std::string serr;
    
            if (shm_unlink(m_name.c_str()) == -1)
                serr = (std::string) " && shm_unlink-" + strerror(errno);
                
            close(shmfd);
            throw std::runtime_error((std::string) "ftruncate-" + strerror(savedErrno) + serr);
        }
    
        m_header = (ShqueHeader *) mmap(NULL, memSize, PROT_READ | PROT_WRITE, MAP_SHARED, shmfd, 0);
        
        if (m_header == MAP_FAILED)
        {
            m_header = NULL;

            std::string serr = (std::string) "mmap-" + strerror(errno);
    
            if (shm_unlink(m_name.c_str()) == -1)
                serr += (std::string) " && shm_unlink-" + strerror(errno);
            
            close(shmfd);
            
            throw std::runtime_error(serr);
        }
    
        close(shmfd);
    
        ///////////////////////////////////////////////////
        //                init semaphore                 //
        ///////////////////////////////////////////////////
        key_t semKey = getKey((std::string) "/tmp" + m_name); /* m_name[0] == '/' */

        if (semKey == -1)
        {
            std::string serr = (std::string) "getKey-" + strerror(errno);

            if (munmap((void *) m_header, memSize) == -1)
                serr += (std::string) " && munmap-" + strerror(errno);

            m_header = NULL;
    
            if (shm_unlink(m_name.c_str()) == -1)
                serr += (std::string) " && shm_unlink-" + strerror(errno);
    
            throw std::runtime_error(serr);
        }
        m_header->semKey = semKey;

        m_semId = semInit(semKey);

        if (m_semId == -1)
        {
            std::string serr = (std::string) "semInit-" + strerror(errno);
    
            if (munmap((void *) m_header, memSize) == -1)
                serr += (std::string) " && munmap-" + strerror(errno);

            m_header = NULL;
    
            if (shm_unlink(m_name.c_str()) == -1)
                serr += (std::string) " && shm_unlink-" + strerror(errno);
    
            throw std::runtime_error(serr);
        }
    
        ///////////////////////////////////////////////////
        //                 init blocks                   //
        ///////////////////////////////////////////////////
        size_t i;
        char *ptr = (char *) m_header;
        char *firstBlock = (char *) &ptr[headerSize];
        Block *blk;
    
        for (i = 0; i < queueMax; ++i)
        {
            blk = (Block *) &firstBlock[i * blockSize];
            blk->nextWritableIdx = i + 1;
            blk->nextReadableIdx = -1;
        }
    
        blk = (Block *) &firstBlock[(i - 1) * blockSize];
        blk->nextWritableIdx = -1;
    
        m_block = (Block *) firstBlock;
    
        /*
         * readHead.iNc[] is lastly read block index not for current readable block index
         * but readHead.iNc[] & 0x70000000 == mean readable block index
         */
        m_header->readHead.iNc[0] = 0;
        m_header->writeHead.iNc[0] = 0;
    
        m_header->memSize    = memSize;
        m_header->headerSize = headerSize;
        m_header->blockSize  = blockSize;

        /* just below two variable must be initiated at last */
        m_header->queueSize = queueSize;
        m_header->queueMax  = queueMax;
    
        return;
    }
    else /* queueSize == 0 || queueMax == 0 */
    {
shm_get:
        ///////////////////////////////////////////////////
        //             get inited semaphore              //
        ///////////////////////////////////////////////////
        shmfd = shm_open(m_name.c_str(), O_RDWR, 0);

        if (shmfd == -1)
        {
            if (errno == ENOENT)
            {
                if (queueSize == 0 || queueMax == 0)
                    throw std::invalid_argument("Queue's size and max num is required for creating");
                else
                    goto shm_create;
            }
    
            throw std::runtime_error((std::string) "shm_open-" + strerror(errno));
        }
        
        struct stat st;
        for (;;)
        {
            if (fstat(shmfd, &st) == -1)
            {
                int savedErrno = errno;
                close(shmfd);

                throw std::runtime_error((std::string) "fstat-" + strerror(savedErrno));
            }
    
            if (st.st_size == 0)
            {
                struct timespec ts;

                ts.tv_sec = 1;
                ts.tv_nsec = 0;

                nanosleep(&ts, NULL); // TODO: handle error

                continue;
            }
    
            break;
        }
    
        m_header = (ShqueHeader *) mmap(NULL, st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, shmfd, 0);

        savedErrno = errno;
        close(shmfd);
    
        if ((void *) m_header == MAP_FAILED)
        {
            m_header = NULL;

            throw std::runtime_error((std::string) "mmap-" + strerror(savedErrno));
        }
    
        /* wait for initiating shared memory */
        while (m_header->queueSize == 0 || m_header->queueMax == 0)
        {
            struct timespec ts;

            ts.tv_sec = 1;
            ts.tv_nsec = 0;

            nanosleep(&ts, NULL); // TODO: handle error

            continue;
        }
     
        ///////////////////////////////////////////////////
        //             check queue config                //
        ///////////////////////////////////////////////////
        if (queueSize != 0 && queueSize != m_header->queueSize)
        {
            std::string serr("not equal to shared queue size");
    
            if (munmap((void *) m_header, st.st_size) == -1)
                serr += (std::string) " && munmap-" + strerror(errno);

            m_header = NULL;
    
            throw std::runtime_error(serr);
        }
    
        if (queueMax != 0 && queueMax != m_header->queueMax)
        {
            std::string serr("not equal to shared queue max");
    
            if (munmap((void *) m_header, st.st_size) == -1)
                serr += (std::string) " && munmap-" + strerror(errno);

            m_header = NULL;
    
            throw std::runtime_error(serr);
        }
    
        ///////////////////////////////////////////////////
        //                get semaphore                  //
        ///////////////////////////////////////////////////
        m_semId = semget(m_header->semKey, 2, SHQ_SEM_PERM);

        if (m_semId == -1)
        {
            std::string serr = (std::string) "semget-" + strerror(errno);

            if (errno == ENOENT)
            {
                m_semId = semInit(m_header->semKey);

                if (m_semId != -1)
                {
                    char *ptr = (char *) m_header;
                    m_block = (Block *) &ptr[m_header->headerSize];
    
                    return;
                }

                serr += (std::string) " && semInit-" + strerror(errno);
            }

            if (munmap((void *) m_header, st.st_size) == -1)
                serr += (std::string) " && munmap-" + strerror(errno);

            m_header = NULL;

            throw std::runtime_error(serr);
        }
    
        char *ptr = (char *) m_header;
        m_block = (Block *) &ptr[m_header->headerSize];
    
        return;
    }
}

size_t
Shque::read(void *dst, size_t size, int blocking)
{
    if (m_header == NULL)
        throw std::runtime_error((std::string) "uninitiated shared queue");

    if (dst == NULL || size == 0)
        throw std::invalid_argument("empty buffer");

    if (size < m_header->queueSize)
        throw std::out_of_range("buffer size is less than queue size");

    uint32_t index = dequeReadableBlock(blocking);

    if (index == (uint32_t) -1)
    {
        int savedErrno = errno;

        /* 
         * queue empty state
         * EAGAIN returned only blocking == 0
         */
        if (savedErrno == EAGAIN)
            return 0;

        throw std::runtime_error((std::string) "dequeReadableBlock-" + strerror(savedErrno));
    }

    char *ptr = (char *) m_block;
    Block *blk = (Block *) &ptr[index * m_header->blockSize];
    size_t dataLen = blk->dataLen;

    if (dataLen > size) /* undefined error */
    {
        queueReadableBlock(index); /* re-queue unread block */
        exit(EXIT_FAILURE); // TODO: distinguish valid exit number
    }
    
    memcpy(dst, (void *) blk->data, dataLen);

    if (queueWritableBlock(index) == -1)
        throw std::runtime_error((std::string) "queueWritableBlock-" + strerror(errno));

    return dataLen;
}

int
Shque::queueReadableBlock(uint32_t index)
{
    uint32_t originIdx = m_header->readHead.iNc[0];
    size_t blockSize = m_header->blockSize;

    char *ptr = (char *) m_block;
    Block *prevBlock = (Block *) &ptr[(originIdx & 0x7fffffff) * blockSize];
    uint32_t nextReadableIdx = prevBlock->nextReadableIdx;

    if (originIdx == index)
    {
        if (nextReadableIdx == (uint32_t) -1) /* if has no next linked block */
        {
            /* 
             * index | 0x80000000 is for making readable block 
             * originally readHead.iNc[0] represents the lastly read index
             */
            uint32_t newIdx = originIdx | 0x80000000;

            if (__sync_bool_compare_and_swap(&m_header->readHead.iNc[0], originIdx, newIdx) != 0)
            {
                /* if succeed to swap */

                if (m_header->blockedReadCount != 0)
                {
                    if (awakeBlockedRead() == -1)
                        return -1;
                }

                return 0; 
            }
            
            /* if reach this line 'readHead.iNc[0]' is not equal to parameter 'index' */
        }
        else /* if has next linked block */
        {
            uint32_t newIdx = nextReadableIdx | 0x80000000;
            /* 
             * don't care below function's result
             * success/failure both bring the desired results
             */
            __sync_bool_compare_and_swap(&m_header->readHead.iNc[0], originIdx, newIdx);
        }

        originIdx = m_header->readHead.iNc[0];
        prevBlock = (Block *) &ptr[(originIdx & 0x7fffffff) * blockSize];
    }
    
    Block *writedBlock = (Block *) &ptr[index * blockSize];
    writedBlock->nextReadableIdx = -1;

    do {
        nextReadableIdx = prevBlock->nextReadableIdx;

        /* find last linked block in read queue */
        while (nextReadableIdx != (uint32_t) -1)
        {
            prevBlock = (Block *) &ptr[nextReadableIdx * blockSize];
            nextReadableIdx = prevBlock->nextReadableIdx;
        } 

    } while (__sync_bool_compare_and_swap(&prevBlock->nextReadableIdx, (uint32_t) -1, index) == 0);

    if (m_header->blockedReadCount != 0)
    {
        if (awakeBlockedRead() == -1)
            return -1;
    }

    return 0;
}

uint32_t
Shque::dequeReadableBlock(int blocking)
{
    union Head originHead, newHead;
    uint32_t index;
    Block *block;

    char *ptr = (char *) m_block;

deque_readable_block:
    originHead.l = m_header->readHead.l;
    
    /* 
     * Head.iNc[0] & 0x80000000
     *      == 1 : readable block index (i.e. 'nextIdx' of lastly read block)
     *      == 0 : lastly read block index
     *
     */
    index = originHead.iNc[0];

    /* if (index & 0x80000000)  if head index is readable block index */
    if ((int32_t) index < 0)
    {
        /* make iNc[0] to lastly read block index */
        newHead.iNc[0] = index & 0x7fffffff;
        newHead.iNc[1] = originHead.iNc[1] + 1;
    }
    else /* if head index is lastly read block index */
    {
        block = (Block *) &ptr[index * m_header->blockSize];

        newHead.iNc[0] = block->nextReadableIdx;
        newHead.iNc[1] = originHead.iNc[1] + 1;

        if (newHead.iNc[0] == (uint32_t) -1) /* if has no readable block */
        {
            if (blocking == 0)
            {
                errno = EAGAIN;
                return (uint32_t) -1;
            }

            if (waitReadable() == -1)
                return (uint32_t) -1;

            goto deque_readable_block;
        }
    }
    
    if (__sync_bool_compare_and_swap(&m_header->readHead.l, originHead.l, newHead.l) == 0)
        goto deque_readable_block;

    return newHead.iNc[0];
}

int
Shque::waitReadable(void)
{
    struct timespec ts;
    struct sembuf sb[2];
    unsigned int blockedReadCount;
    int s, savedErrno;
    uint32_t headIndex;
    char *ptr;
    Block *blk;

    sb[0].sem_num = 0;
    sb[0].sem_op = 0;
    sb[0].sem_flg = 0;

    sb[1].sem_num = 0;
    sb[1].sem_op = 1;
    sb[1].sem_flg = 0;

    ptr = (char *) m_block;
    ts.tv_nsec = 0;

    for (;;)
    {
        headIndex = m_header->readHead.iNc[0];
        /* if (headIndex & 0x80000000) */
        if ((int32_t) headIndex < 0)
        {
            return 0;
        }
        else
        {
            blk = (Block *) &ptr[headIndex * m_header->blockSize];
            if (blk->nextReadableIdx != (uint32_t) -1)
                return 0;
        }

        do {
            blockedReadCount = __sync_add_and_fetch(&m_header->blockedReadCount, (unsigned int) 1);
        } while (blockedReadCount == 0);
        
        ts.tv_sec = blockedReadCount > INT_MAX ? INT_MAX : blockedReadCount;

        s = semtimedop(m_semId, sb, 2, &ts);

        if (s == -1)
        {
            savedErrno = errno;

            switch (savedErrno)
            {
            case EAGAIN:
            case EINTR:
                continue;

            case ERANGE: /* reach SEMVMX */
                return 0;

            default:
                return -1;
            }
        }

        return 0;
    }
}

int
Shque::awakeBlockedRead(void)
{
    m_header->blockedReadCount = 0;

    if (semctl(m_semId, 0, SETVAL, 0) == -1)
        return -1;

    return 0;
}

size_t
Shque::write(void* src, size_t size, int blocking)
{
    if (m_header == NULL)
        throw std::runtime_error((std::string) "uninitiated shared queue");

    if (src == NULL || size == 0)
        throw std::invalid_argument("empty data");

    if (size > m_header->queueSize) /* undefined error */
        throw std::out_of_range((std::string) "data size is greater than queue size");

    uint32_t index = dequeWritableBlock(blocking);

    if (index == (uint32_t) -1) 
    {
        int savedErrno = errno;

        /* 
         * queue full state
         * EAGAIN returned only blocking == 0
         */
        if (savedErrno == EAGAIN)
            return 0;

        throw std::runtime_error((std::string) "dequeWritableBlock-" + strerror(savedErrno));
    }

    char *ptr = (char *) m_block;
    Block *blk = (Block *) &ptr[index * m_header->blockSize];

    memcpy((void *) blk->data, src, size);
    blk->dataLen = size;

    if (queueReadableBlock(index) == -1)
        throw std::runtime_error((std::string) "queueReadableBlock-" + strerror(errno));

    return size;
}

int
Shque::queueWritableBlock(uint32_t index)
{
    char *ptr = (char *) m_block;
    Block *blk = (Block *) &ptr[index * m_header->blockSize];
    uint32_t originIdx;

    do {
        originIdx = m_header->writeHead.iNc[0];
        blk->nextWritableIdx = originIdx;
    } while (__sync_bool_compare_and_swap(&m_header->writeHead.iNc[0], originIdx, index) == 0);

    if (m_header->blockedWriteCount != 0)
    {
        if (awakeBlockedWrite() == -1)
            return -1; 
    }
    
    return 0;
}

uint32_t
Shque::dequeWritableBlock(int blocking)
{
    union Head originHead, newHead;
    char *ptr;
    uint32_t index;
    Block *block;
    size_t blockSize = m_header->blockSize;
    
deque_writable_block:
    originHead.l = m_header->writeHead.l;
    index = originHead.iNc[0];

    if (index == (uint32_t) -1) /* if has no writable block */
    {
        if (blocking == 0)
        {
            errno = EAGAIN;
            return (uint32_t) -1;
        }

        if (waitWritable() == -1)
            return (uint32_t) -1;

        goto deque_writable_block;
    }
    else /* if has writable block */
    {
        ptr = (char *) m_block;
        block = (Block *) &ptr[index * blockSize];

        newHead.iNc[0] = block->nextWritableIdx;
        newHead.iNc[1] = originHead.iNc[1] + 1;
    }

    if (__sync_bool_compare_and_swap(&m_header->writeHead.l, originHead.l, newHead.l) == 0)
        goto deque_writable_block;

    return index;
}

int
Shque::waitWritable(void)
{
    struct timespec ts;
    struct sembuf sb[2];
    unsigned int blockedWriteCount;
    int s, savedErrno;

    sb[0].sem_num = 1;
    sb[0].sem_op = 0;
    sb[0].sem_flg = 0;

    sb[1].sem_num = 1;
    sb[1].sem_op = 1;
    sb[1].sem_flg = 0;

    ts.tv_nsec = 0;

    for (;;)
    {
        if (m_header->writeHead.iNc[0] != (uint32_t) -1)
            return 0;

        do {
            blockedWriteCount = __sync_add_and_fetch(&m_header->blockedWriteCount, (unsigned int) 1);
        } while (blockedWriteCount == 0);

        ts.tv_sec = blockedWriteCount > INT_MAX ? INT_MAX : blockedWriteCount;

        s = semtimedop(m_semId, sb, 2, &ts);

        if (s == -1)
        {
            savedErrno = errno;

            switch (savedErrno)
            {
            case EAGAIN:
            case EINTR:
                continue;

            case ERANGE: /* reach SEMVMX */
                return 0;

            default:
                return -1;
            }
        }

        return 0;
    }
}

int
Shque::awakeBlockedWrite(void)
{
    m_header->blockedWriteCount = 0;

    if (semctl(m_semId, 1, SETVAL, 0) == -1)
        return -1;
    
    return 0;
}

static
key_t
getKey(std::string fileName)
{
    int fd = c_open(fileName.c_str(), O_CREAT | O_RDONLY, S_IRUSR | S_IRGRP);

    if (fd == -1)
        return -1;

    key_t key = ftok(fileName.c_str(), 's');

    int savedErrno = errno;
    close(fd);

    if (key == -1)
    {
        errno = savedErrno;
        return -1;
    }
    
    return key;
}

int
Shque::semInit(key_t key)
{
    // TODO: handle if exist
    int semId = semget(key, 2, IPC_CREAT | SHQ_SEM_PERM);

    if (semId == -1)
        return -1;

    union semun {
        int val;
        struct semid_ds *buf;
        unsigned short array[2];
    };

    union semun su;
    /* used for queue empty/full state */
    su.array[0] = 1; /* for read queue (empty state) */
    su.array[1] = 1; /* for write queue (full state) */

    if (semctl(semId, 2, SETALL, &su) == -1)
    {
        int savedErrno = errno;

        semctl(semId, 0, IPC_RMID);
        errno = savedErrno;

        return -1;
    }

    return semId;
}

