
#include "mem2file.h"

#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>

/*********** MACROS ***********/

#define DEBUG

#ifdef DEBUG
#define _debug(x...) do {printf("[debug][%s %d %s]", \
	__FILE__,__LINE__,__FUNCTION__);printf(x);} while (0)
#define _info(x...) do {printf("[info][%s %d %s]", \
	__FILE__,__LINE__,__FUNCTION__);printf(x);} while (0)
#define _error(x...) do {printf("[error][%s %d %s]", \
	__FILE__,__LINE__,__FUNCTION__);printf(x);} while (0)
#else 
#define _debug(x...) do {;} while (0)
#define _info(x...) do {printf("[info][%s %d %s]", \
	__FILE__,__LINE__,__FUNCTION__);printf(x);} while (0)
#define _error(x...) do {printf("[error][%s %d %s]", \
	__FILE__,__LINE__,__FUNCTION__);printf(x);} while (0)
#endif 

/* 打印精确到毫秒的时间 */
#define _debug_timeprint() \
	do { \
		struct timeval tvTmp = {}; \
        struct tm stmTmp = {}; \
        char szDTime[64] = {}; \
        char szResult[64] = {}; \
        \
		gettimeofday (& tvTmp, NULL); \
        gmtime_r (& tvTmp.tv_sec, & stmTmp); \
		strftime (szDTime, sizeof(szDTime), "%F %T", &stmTmp); \
        snprintf (szResult, sizeof(szResult), "%s.%03d", szDTime, (int)(tvTmp.tv_usec / 1000)); \
        \
        printf ("timestamp[%s %d %s] %s\n", __FILE__,__LINE__,__FUNCTION__, szResult); \
	} while (0)

/*********** TYPES ***********/

typedef struct 
{
    int fd;
} MEM2FILE_Obj;

/*********** STATIC FUNCS ***********/

static int mem2file_getfilesize (int fd, int *pnSize)
{
    struct stat sStat = {};
    if (fstat (fd, & sStat) < 0)
    {
        _error ("stat failed\n");
        return -1;
    }

    *pnSize = sStat.st_size;
    return 0;
}

/*********** GLOBAL FUNCS ***********/

/**
 * @note 单进单出
 */
MEM2FILE_HANDLE mem2file_create(const char *szFileName)
{
    int bError = 0;

    /* 打开文件 */
    int fd = 0;
    if (0 == bError)
    {
        fd = open (szFileName, O_RDWR | O_CREAT, 0664);
        if (fd < 0)
        {
            _error ("open <%s> failed\n", szFileName);
            bError = 1;
        }
        else 
        {
            _debug ("open <%s> successful\n", szFileName);
        }
    }

    /* 建立对象 */
    MEM2FILE_Obj *pObj = NULL;
    if (0 == bError)
    {
        void *pMem = malloc (sizeof(MEM2FILE_Obj));
        if (NULL == pMem)
        {
            _error ("malloc failed\n");
            bError = 1;
        }
        else 
        {
            _debug ("malloc successful, p=%p\n", pMem);
            pObj = (MEM2FILE_Obj*)pMem;
            pMem = NULL;
        }
    }

    /* 生成对象 */
    if (0 == bError)
    {
        pObj->fd = fd;
    }

    /* 错误处理 */
    if (bError)
    {
        if (fd >= 0)
        {
            _debug ("close fd = %d\n", fd);
            close (fd);
            fd = -1;
        }
        if (pObj != NULL)
        {
            _debug ("free %p\n", pObj);
            pObj = NULL;
        }
    }

    return (MEM2FILE_HANDLE)pObj;
}

/**
 * 
 */
int mem2file_close(MEM2FILE_HANDLE hInstance)
{
    MEM2FILE_Obj *pObj = (MEM2FILE_Obj*)hInstance;

    if (NULL == pObj)
    {
        _error ("close null\n");
        return -1;
    }

    if (pObj->fd >= 0)
    {
        _debug ("close fd = %d\n", pObj->fd);
        close (pObj->fd);
        pObj->fd = -1;
    }
    else 
    {
        _error ("inner error, fd = %d\n", pObj->fd);
    }

    _debug ("free mem=%p\n", pObj);
    free (pObj);

    return 0;
}

int mem2file_size (MEM2FILE_HANDLE hInstance, int *pnSize)
{
    MEM2FILE_Obj *pObj = (MEM2FILE_Obj*)hInstance;

    if (NULL == pObj)
    {
        _error ("null obj\n");
        return -1;
    }

    return mem2file_getfilesize (pObj->fd, pnSize);
}

int mem2file_resize (MEM2FILE_HANDLE hInstance, int nSize)
{
    MEM2FILE_Obj *pObj = (MEM2FILE_Obj*)hInstance;

    if (NULL == pObj)
    {
        _error ("null obj\n");
        return -1;
    }

    if (ftruncate (pObj->fd, nSize) < 0)
    {
        _error ("truncate failed\n");
        return -1;
    }
    return 0;
}

int mem2file_setdata (MEM2FILE_HANDLE hInstance, int pos, const void *pData, int nSize)
{
    MEM2FILE_Obj *pObj = (MEM2FILE_Obj*)hInstance;

    if (NULL == pObj)
    {
        _error ("null obj\n");
        return -1;
    }

    int nFileSize = 0;
    if (mem2file_getfilesize (pObj->fd, &nFileSize) < 0)
    {
        _error ("get file size failed\n");
        return -1;
    }

    if (pos + nSize > nFileSize)
    {
        _error ("param error<pos=%d,size=%d,total=%d>\n", pos, nSize, nFileSize);
        return -1;
    }

    if (lseek (pObj->fd, pos, SEEK_SET) < 0)
    {
        _error ("lseek failed\n");
        return -1;
    }

    const int ret_write = write (pObj->fd, pData, nSize);
    if (ret_write != nSize)
    {
        _error ("set data to file failed or error\n");
        return -1;
    }

    return 0;
}

int mem2file_getdata (MEM2FILE_HANDLE hInstance, int pos, void *pData, int nSize)
{
    MEM2FILE_Obj *pObj = (MEM2FILE_Obj*)hInstance;

    if (NULL == pObj)
    {
        _error ("null obj\n");
        return -1;
    }

    int nFileSize = 0;
    if (mem2file_getfilesize (pObj->fd, &nFileSize) < 0)
    {
        _error ("get file size failed\n");
        return -1;
    }

    if (pos + nSize > nFileSize)
    {
        _error ("<pos=%d,size=%d,total=%d>\n", pos, nSize, nFileSize);
        return -1;
    }

    if (lseek (pObj->fd, pos, SEEK_SET) < 0)
    {
        _error ("lseek failed\n");
        return -1;
    }

    const int ret_read = read (pObj->fd, pData, nSize);
    if (ret_read != nSize)
    {
        _error ("get data from file failed or error\n");
        return -1;
    }

    return 0;
}

int mem2file_sync (MEM2FILE_HANDLE hInstance)
{
    MEM2FILE_Obj *pObj = (MEM2FILE_Obj*)hInstance;

    if (NULL == pObj)
    {
        _error ("null obj\n");
        return -1;
    }

    if (fsync (pObj->fd) < 0)
    {
        _error ("fsync failed\n");
        return -1;
    }

    return 0;
}