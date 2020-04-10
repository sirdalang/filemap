
#include "filemap.h"

#include <pthread.h>

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "mem2file.h"
#include "hash.h"

/************ MACROS ************/

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

#define FILEMAP_VERSION "FILEMAP V1.0"

#define INDEX_NULL (-1)

/************ TYPES ************/

typedef struct 
{
    int pos;
    int size;
} FILEMAP_SEGMENT;

typedef struct 
{
    FILEMAP_SEGMENT seg;
} FILEMAP_DEF_MAP;

typedef struct 
{
    FILEMAP_SEGMENT seg;
} FILEMAP_INDEX_BITMAP_MAP;

typedef struct 
{
    FILEMAP_SEGMENT seg;
} FILEMAP_INDEX_POSHASHMAP_MAP;

typedef struct 
{
    FILEMAP_SEGMENT seg;
} FILEMAP_INDEX_HASHLINK_MAP;

typedef struct 
{
    FILEMAP_SEGMENT seg;
    FILEMAP_INDEX_BITMAP_MAP seg_bitmap_data;
    FILEMAP_INDEX_BITMAP_MAP seg_bitmap_hashlink;
    FILEMAP_INDEX_POSHASHMAP_MAP seg_hashmap;
    FILEMAP_INDEX_HASHLINK_MAP seg_hashlink;
} FILEMAP_INDEX_MAP;

typedef struct 
{
    FILEMAP_SEGMENT seg;
} FILEMAP_DATA_MAP;

/* 各段地图 */
typedef struct 
{
    FILEMAP_SEGMENT seg;
    FILEMAP_DEF_MAP seg_def;
    FILEMAP_INDEX_MAP seg_index;
    FILEMAP_DATA_MAP seg_data;
} FILEMAP_GLOBAL_MAP;

/* 定义区结构 */
typedef struct 
{
    char szVersion[16];
    int nMaxFileNum;
} FILEMAP_SECTION_DEF;

typedef struct 
{
    int bUsedFlag; // 在哈希表中标记是否使用
    FILEMAP_KEY key;
    int nIndex; // 该项在数据段中的索引位置

    int nNextIndex; // 链表索引
} FILEMAP_DATAMAP;

/* 位置哈希表元素 */
typedef struct 
{
    FILEMAP_DATAMAP node;
} FILEMAP_POSHASHMAP_ELEMENT;

/* 位置哈希链表元素 */
typedef struct 
{
    FILEMAP_DATAMAP node;
} FILEMAP_POSHASHLINKMAP_ELEMENT;

/* 数据段元素 */
typedef struct 
{
    FILEMAP_VALUE value;
} FILEMAP_SECTION_DATA_ELEMENT;

/* 主对象 */
typedef struct 
{
    MEM2FILE_HANDLE hMem2File;
    int nMaxFileNum;
    pthread_mutex_t mutex_entrance_call;
} FILEMAP_OBJ;


/************ FUNCTION_DELARATION ************/
static int filemap_get_defseg (MEM2FILE_HANDLE hMem2File, FILEMAP_SECTION_DEF *psDef);
static int filemap_set_defseg (MEM2FILE_HANDLE hMem2File, const FILEMAP_SECTION_DEF *psDef);
static int filemap_check_version (MEM2FILE_HANDLE hMem2File);
static int filemap_check_compatibility (MEM2FILE_HANDLE hMem2File, int nMaxFileNum);
static int filemap_init_defsec (MEM2FILE_HANDLE hMem2File, int nMaxFileNum);
static FILEMAP_HANDLE filemap_init_file (const char *szFileName, int nMaxFileNum);
static int filemap_close_file (FILEMAP_HANDLE hInstance);
static int filemap_file_existitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key);
static int filemap_getsegmap (int nMaxFileNum, FILEMAP_GLOBAL_MAP *psMap);
static int filemap_scanfirstemptybit (const char *pMem, int size, int *pnIndex);
static int filemap_setbitofmem (char *pMem, int nSize, int nIndex, int bitValue);
static int filemap_file_scanfirstemptybit (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nPos, int nSize, int *pnIndex);
static int filemap_file_setbitmap (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nPos, int nSize, int nIndex, int bBit);
static int filemap_file_getposhashmapitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, FILEMAP_POSHASHMAP_ELEMENT *pEle);
static int filemap_file_setposhashmapitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, const FILEMAP_POSHASHMAP_ELEMENT *pEle);
static int filemap_file_getposhashlinkitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, FILEMAP_POSHASHLINKMAP_ELEMENT *pEle);
static int filemap_file_setposhashlinkitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, const FILEMAP_POSHASHLINKMAP_ELEMENT *pEle);
static int filemap_file_getdatasegitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, FILEMAP_SECTION_DATA_ELEMENT *pElem);
static int filemap_file_setdatasegitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, const FILEMAP_SECTION_DATA_ELEMENT *pElem);
static int filemap_file_getdatamap(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key, FILEMAP_DATAMAP *map);
static int filemap_file_adddatamap(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_DATAMAP *map);
static int filemap_file_deldatamap(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key);
static int filemap_hashmap_getindex (int nMaxFileNum, const FILEMAP_KEY *key);
static int filemap_keycmp (const FILEMAP_KEY *keyA, const FILEMAP_KEY *keyB);
static int filemap_getdefsegmap (FILEMAP_DEF_MAP *psMap);
static int filemap_file_getitem(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key, FILEMAP_VALUE *value);
static int filemap_file_setitem(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key, const FILEMAP_VALUE *value);
static int filemap_file_deleteitem(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key);
static int filemap_entrancecall_lock (FILEMAP_HANDLE hInstance);
static int filemap_entrancecall_unlock (FILEMAP_HANDLE hInstance);
static int filemap_file_generateinfo (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const char *szFileName);

/************ STATIC FUNCS ************/

static int filemap_get_poshashmap_num (int nMaxFileNum)
{
    int nNumEx = nMaxFileNum + nMaxFileNum / 4;
    return nNumEx;
}

/**
 * 查询定义段内容
 */
static int filemap_get_defseg (MEM2FILE_HANDLE hMem2File, FILEMAP_SECTION_DEF *psDef)
{
    FILEMAP_DEF_MAP sMap = {};
    if (filemap_getdefsegmap (&sMap) < 0)
    {
        _error ("get seg map failed\n");
        return -1;
    }

    const int nSegDefPos = sMap.seg.pos;
    const int nSegDefSize = sMap.seg.size;

    int nFileSize = 0;
    if (mem2file_size(hMem2File, &nFileSize) < 0)
    {
        _error ("get size failed\n");
        return -1;
    }

    if (nFileSize < nSegDefSize)
    {
        _error ("size too small\n");
        return -1;
    }

    if (mem2file_getdata(hMem2File, nSegDefPos, psDef, sizeof(FILEMAP_SECTION_DEF)) < 0)
    {
        _error ("get def sec failed\n");
        return -1;
    }

    _debug ("head=<pos=%d,ver=%s,maxfilenum=%d>\n", 
        nSegDefPos, psDef->szVersion, psDef->nMaxFileNum);

    return 0;
}

static int filemap_set_defseg (MEM2FILE_HANDLE hMem2File, const FILEMAP_SECTION_DEF *psDef)
{
    FILEMAP_DEF_MAP sMap = {};
    if (filemap_getdefsegmap (&sMap) < 0)
    {
        _error ("get seg map failed\n");
        return -1;
    }

    const int nSegDefSize = sMap.seg.size;

    int nFileSize = 0;
    if (mem2file_size(hMem2File, &nFileSize) < 0)
    {
        _error ("get size failed\n");
        return -1;
    }

    if (nFileSize < nSegDefSize)
    {
        _error ("size too small\n");
        return -1;
    }

    if (mem2file_setdata (hMem2File, 0, psDef, sizeof(*psDef)) < 0)
    {
        _error ("set def sec failed\n");
        return -1;
    }

    return 0;
}

/**
 * @brief 检查一个已有对象的版本号
 * @return 兼容返回0，否则返回-1
 */
static int filemap_check_version (MEM2FILE_HANDLE hMem2File)
{
    FILEMAP_SECTION_DEF sDef = {};
    if (filemap_get_defseg (hMem2File, &sDef) < 0)
    {
        _error ("get def sec failed\n");
        return -1;
    }

    if (strcmp (sDef.szVersion, FILEMAP_VERSION) != 0)
    {
        _info ("version not same, <%s,%s>\n", sDef.szVersion, FILEMAP_VERSION);
        return -1;
    }

    return 0;
}

/**
 * @brief 检查与旧文件的兼容性
 * @return 兼容返回0，否则返回-1
 */
static int filemap_check_compatibility (MEM2FILE_HANDLE hMem2File, int nMaxFileNum)
{
    FILEMAP_SECTION_DEF sDef = {};
    if (filemap_get_defseg (hMem2File, & sDef) < 0)
    {
        _error ("get def sec failed\n");
        return -1;
    }
    
    if (nMaxFileNum != -1 && sDef.nMaxFileNum != nMaxFileNum)
    {
        return -1;
    }

    return 0;
}

/**
 * @brief 填充定义区信息
 */
static int filemap_init_defsec (MEM2FILE_HANDLE hMem2File, int nMaxFileNum)
{
    FILEMAP_GLOBAL_MAP sGMap = {};
    if (filemap_getsegmap (nMaxFileNum, &sGMap) < 0)
    {
        _error ("get seg map failed\n");
        return -1;
    }

    const int nSegDefSize = sGMap.seg_def.seg.size;
    // const int nSegIndexSize = sGMap.seg_index.seg.size;

    int nFileSize = 0;
    if (mem2file_size(hMem2File, &nFileSize) < 0)
    {
        _error ("get size failed\n");
        return -1;
    }

    if (nFileSize < nSegDefSize)
    {
        _error ("size too small\n");
        return -1;
    }

    FILEMAP_SECTION_DEF sDef = {
        FILEMAP_VERSION,
        nMaxFileNum,
    };

    if (filemap_set_defseg (hMem2File, &sDef) < 0)
    {
        _error ("set data failed\n");
        return -1;
    }

    return 0;
}

/**
 * @brief 根据给定的文件名，创建一个已经初始化了的文件映射，并
 * 返回文件映射句柄
 * @param nMaxFileNum 最大文件数量，如果为-1，则从旧文件加载
 * @note 单进单出
 */
static FILEMAP_HANDLE filemap_init_file (const char *szFileName, int nMaxFileNum)
{
    int bError = 0;


    /* 文件转换为mem2file */
    MEM2FILE_HANDLE hMem2File = NULL;
    if (0 == bError)
    {
        hMem2File = mem2file_create (szFileName);
        if (NULL == hMem2File)
        {
            _error ("create mem2file failed\n");
            bError = 1;
        }
    }

    /* 获取对象大小 */
    int nOriginalFileSize = 0;
    if (0 == bError)
    {
        if (mem2file_size(hMem2File, &nOriginalFileSize) < 0)
        {
            _error ("get size failed\n");
            bError = 1;
        }
        else 
        {
            _debug ("file size = %d\n", nOriginalFileSize);
        }
    }

    /* 空文件 */
    int bNeedReinitialize = 0;
    if (0 == bNeedReinitialize && 0 == bError)
    {
        if (0 == nOriginalFileSize)
        {
            _info ("file empty, need reinitialize\n");
            bNeedReinitialize = 1;
        }
    }

    /* 检查版本号 */
    if (0 == bNeedReinitialize && 0 == bError)
    {
        if (filemap_check_version (hMem2File) < 0)
        {
            _info ("version not compatible, need reinitialize\n");
            bNeedReinitialize = 1;
        }
    }

    /* 检查旧文件兼容性 */
    if (0 == bNeedReinitialize && 0 == bError)
    {   
        if (filemap_check_compatibility (hMem2File, nMaxFileNum) < 0)
        { /* 如果不兼容 */
            _info ("new setting not compatible to old, need reinitialize\n");
            bNeedReinitialize = 1;
        }
        else 
        { 
            if (nMaxFileNum == -1)
            { /* 特殊情况 */
                FILEMAP_SECTION_DEF sDefSeg = {};
                if (filemap_get_defseg (hMem2File, &sDefSeg) < 0)
                {
                    bNeedReinitialize = 1;
                }
                else 
                {
                    nMaxFileNum = sDefSeg.nMaxFileNum; /* 从旧文件获取 */
                }
            }
        }
    }

    FILEMAP_GLOBAL_MAP sGMap = {};
    if (0 == bError)
    {
        if (filemap_getsegmap (nMaxFileNum, &sGMap) < 0)
        {
            _error ("get seg map failed\n");
            bError = 1;
        }
    }

    const int nSegDefSize = sGMap.seg_def.seg.size;
    const int nSegIndexSize = sGMap.seg_index.seg.size;
    
    /* 初始化的时候，不填充数据段，避免过多耗时 */
    const int nInitialSize = nSegDefSize + nSegIndexSize;

    /* 处理不兼容版本 */
    if (0 == bError)
    {
        if (bNeedReinitialize)
        {
            if (mem2file_resize (hMem2File, 0) < 0)
            {
                _error ("resize failed\n");
                bError = 1;
            }
            if (mem2file_resize (hMem2File, nInitialSize) < 0)
            {
                _error ("resize failed\n");
                bError = 1;
            }
            if (filemap_init_defsec (hMem2File, nMaxFileNum) < 0)
            {
                _error ("init def sec failed\n");
                bError = 1;
            }
            _info ("reinitialize successful\n");
        }
    }

    /* 创建文件映射对象 */
    FILEMAP_HANDLE hFileMap = NULL;
    if (0 == bError)
    {
        void *pMem = malloc (sizeof(FILEMAP_OBJ));
        if (NULL == pMem)
        {
            _error ("malloc failed\n");
            bError = 1;
        }
        else 
        {
            _debug ("malloc successful, p=%p\n", pMem);
            hFileMap = pMem;
            pMem = NULL;
        }
    }

    /* 初始化锁 */
    if (0 == bError)
    {
        pthread_mutex_t mutexTmp = PTHREAD_MUTEX_INITIALIZER;
        FILEMAP_OBJ *pObj = (FILEMAP_OBJ*)hFileMap;

        pObj->mutex_entrance_call = mutexTmp;
    }

    /* 填充文件映射对象 */
    if (0 == bError)
    {
        FILEMAP_OBJ *pObj = (FILEMAP_OBJ*)hFileMap;
        pObj->hMem2File = hMem2File;
        pObj->nMaxFileNum = nMaxFileNum;
        hMem2File = NULL;
    }

    /* 错误处理 */
    if (bError)
    {
        if (hFileMap != NULL)
        {
            FILEMAP_OBJ *pObj = (FILEMAP_OBJ*)hFileMap;
            if (pObj->hMem2File != NULL)
            {
                mem2file_close (pObj->hMem2File);
                pObj->hMem2File = NULL;
            }

            free (pObj);
            _debug ("mem freed, p=%p\n", pObj);
        }
        if (hMem2File != NULL)
        {
            mem2file_close (hMem2File);
            hMem2File = NULL;
        }
    }

    /* 报告 */
    if (bError)
    {
        _error ("init file<%s> failed\n", szFileName);
    }

    return hFileMap;
}

static int filemap_close_file (FILEMAP_HANDLE hInstance)
{
    FILEMAP_OBJ *pObj = (FILEMAP_OBJ*)hInstance;

    if (NULL == pObj)
    {
        _error ("close null\n");
    }
    else 
    {
        if (NULL == pObj->hMem2File)
        {
            _error ("inner error\n");
        }
        else 
        {
            mem2file_close (pObj->hMem2File);
        }

        _debug ("free mem, p=%p\n", pObj);
        free (pObj);
        pObj = NULL;
    }

    return 0;
}

/**
 * @brief 找出内存块中的第一个空位
 * @return 找到空位返回1，且将索引返回至@pnIndex，否则返回0
 */
static int filemap_scanfirstemptybit (const char *pMem, int size, int *pnIndex)
{
    int bFound = 0;

    /* 先逐个字节判断，后期优化 */
    for (int i = 0; i < size; ++i)
    {
        char cByte = *(pMem + i);

        // _debug ("cByte=%#x\n", cByte);

        if (cByte != (char)0xFF)
        {
            for (int k = 0; k < sizeof(char) * 8; ++k)
            {
                char bitMask = ((char)0x1 << (7-k));
                // _debug ("bitMask=%#x\n", bitMask);

                if ( (bitMask & cByte) == 0)
                {
                    bFound = 1;
                    *pnIndex = i * sizeof (char) * 8 + k;
                    break;
                }
            }
            break;
        }
    }

    return bFound ? 1 : 0;
}

/**
 * @brief 设置内存块中的第@nIndex个bit为@bitValue
 */
static int filemap_setbitofmem (char *pMem, int nSize, int nIndex, int bitValue)
{
    if (nIndex >= nSize * sizeof(char) * 8)
    {
        _error ("index over limit, <index=%d,size=%d>\n", nIndex, nSize);
        return -1;
    }

    const int nIndexByte = nIndex / (sizeof(char) * 8); // 比特索引对应的字节序号
    const int nIndexOfBitInByte = nIndex % (sizeof(char) * 8);    // 比特索引在字节中的索引

    char byteOriginal = *(pMem + nIndexByte);
    
    char byteNew = byteOriginal;

    if (bitValue)
    {
        byteNew = byteOriginal | ((char)0x1 << (7 - nIndexOfBitInByte));
    }
    else 
    {
        char byteMask = ((char)0x1 << (7 - nIndexOfBitInByte));    // eg. 0000 1000
        byteMask = ~byteMask;   // eg. 1111 0111
        byteNew = byteOriginal & byteMask;
    }

    *(pMem + nIndexByte) = byteNew;

    return 0;
}

/**
 * @brief 找出比特表中第一个空缺
 * @return 失败返回-1，成功返回1，@nIndex返回索引值，不存在返回0
 */ 
static int filemap_file_scanfirstemptybit (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nPos, int nSize, int *pnIndex)
{
    FILEMAP_GLOBAL_MAP sMap = {};
    int ret = filemap_getsegmap (nMaxFileNum, &sMap);
    
    const int nBitmapPos = nPos;
    const int nBitmapSize = nSize;

    int bFound = 0;

    int i = 0;
    for (i = 0; ; ++i)
    {
        char byteBuffer[64] = {}; // 这里循环生效，则测试的文件数目要超过 64 * 8 = 512

        const int nPos = nBitmapPos + sizeof(byteBuffer) * i;

        if ( (nPos - nBitmapPos) > nBitmapSize)
        {
            // _debug ("scan finished\n");
            break;
        }

        const int nLeftSize = nBitmapSize - (nPos - nBitmapPos);
        const int nReadSize = (nLeftSize > sizeof(byteBuffer) ? 
                                    sizeof(byteBuffer) : nLeftSize);

        ret = mem2file_getdata (hMem2File, nPos, byteBuffer, nReadSize);
        if (ret < 0)
        {
            _error ("getdata failed, <bitmappos=%d,bitmapsize=%d,pos=%d,size=%d>\n",
                        nBitmapPos, nBitmapSize, nPos, nReadSize);
            return -1;
        }
        
        int nIndex = 0;
        ret = filemap_scanfirstemptybit (byteBuffer, nReadSize, &nIndex);
        if (1 == ret && (nIndex < nMaxFileNum)) /* 由于每个字节8位，因此要考虑找出来的是越界的位的问题 */
        { /* 找到了 */
            bFound = 1;
            *pnIndex = nIndex + i * sizeof(byteBuffer) * 8;
            break;
        }
    }

    return bFound ? 1 : 0;
}

/**
 * @brief 设置比特表中的第@nIndex个位为@bBit
 * @param nPos 比特表的起始位置
 * @param nSize 比特表的大小
 */
static int filemap_file_setbitmap (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nPos, int nSize, int nIndex, int bBit)
{
    FILEMAP_GLOBAL_MAP sMap = {};
    int ret = filemap_getsegmap (nMaxFileNum, &sMap);
    
    const int nBitmapPos = nPos;
    const int nBitmapSize = nSize;

    const int nTotalBit = nBitmapSize * sizeof(char) * 8;

    if (nIndex >= nTotalBit)
    {
        _error ("index over limit, <%d>=%d>\n", nIndex, nTotalBit);
        return -1;
    }

    const int nIndexByte = nIndex / (sizeof(char) * 8); // 比特索引对应的字节序号
    const int nIndexBitInByte = nIndex % (sizeof(char) * 8);    // 比特索引在字节中的序号
    const int nBytePos = nIndexByte + nBitmapPos;   // 对应的实际字节位置

    char byteOriginal = 0;
    ret = mem2file_getdata (hMem2File, nBytePos, & byteOriginal, sizeof(byteOriginal));
    if (ret < 0)
    {
        _error ("get data failed\n");
        return -1;
    }

    ret = filemap_setbitofmem (&byteOriginal, sizeof(byteOriginal), nIndexBitInByte, bBit);
    if (ret < 0)
    {
        _error ("set bit failed\n");
        return -1;
    }

    ret = mem2file_setdata (hMem2File, nBytePos, & byteOriginal, sizeof(byteOriginal));
    if (ret < 0)
    {
        _error ("set data failed\n");
        return -1;
    }

    return 0;
}

/**
 * @brief 获取位置哈希表中的第@nIndex个元素
 */
static int filemap_file_getposhashmapitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, FILEMAP_POSHASHMAP_ELEMENT *pEle)
{
    /* 获取总元素 */
    int nNumEx = filemap_get_poshashmap_num (nMaxFileNum);

    if (nIndex >= nNumEx || nIndex < 0)
    {
        _error ("nIndex invalid, <%d,%d>\n", nIndex, nNumEx);
        return -1;
    }

    /* 获取地图 */
    FILEMAP_GLOBAL_MAP sMap = {};
    if (filemap_getsegmap (nMaxFileNum, & sMap) < 0)
    {
        _error ("get map failed\n");
        return -1;
    }

    /* 得到待获取元素的位置 */
    const int nDataPos = sMap.seg_index.seg_hashmap.seg.pos + 
                    sizeof(FILEMAP_POSHASHMAP_ELEMENT) * nIndex;
    const int nDataSize = sizeof(FILEMAP_POSHASHMAP_ELEMENT);

    if (mem2file_getdata (hMem2File, nDataPos, pEle, nDataSize) < 0)
    {
        _error ("get data failed\n");
        return -1;
    }

    return 0;
}

/**
 * @brief 设置位置哈希表中的第@nIndex个元素
 */
static int filemap_file_setposhashmapitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, const FILEMAP_POSHASHMAP_ELEMENT *pEle)
{
    /* 获取总元素 */
    int nNumEx = filemap_get_poshashmap_num (nMaxFileNum);

    if (nIndex >= nNumEx || nIndex < 0)
    {
        _error ("nIndex invalid, <%d,%d>\n", nIndex, nNumEx);
        return -1;
    }

    /* 获取地图 */
    FILEMAP_GLOBAL_MAP sMap = {};
    if (filemap_getsegmap (nMaxFileNum, & sMap) < 0)
    {
        _error ("get map failed\n");
        return -1;
    }

    /* 得到待获取元素的位置 */
    const int nDataPos = sMap.seg_index.seg_hashmap.seg.pos + 
                    sizeof(FILEMAP_POSHASHMAP_ELEMENT) * nIndex;
    const int nDataSize = sizeof(FILEMAP_POSHASHMAP_ELEMENT);

    if (mem2file_setdata (hMem2File, nDataPos, pEle, nDataSize) < 0)
    {
        _error ("get data failed\n");
        return -1;
    }

    return 0;
}

static int filemap_file_getposhashlinkitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, FILEMAP_POSHASHLINKMAP_ELEMENT *pEle)
{
    if (nIndex < 0 || nIndex >= nMaxFileNum)
    {
        _error ("nIndex invalid, <%d,%d>\n", nIndex, nMaxFileNum);
        return -1;
    }

    /* 获取地图 */
    FILEMAP_GLOBAL_MAP sMap = {};
    if (filemap_getsegmap (nMaxFileNum, & sMap) < 0)
    {
        _error ("get map failed\n");
        return -1;
    }

    /* 得到待获取元素的位置 */
    const int nDataPos = sMap.seg_index.seg_hashlink.seg.pos + 
                    sizeof(FILEMAP_POSHASHLINKMAP_ELEMENT) * nIndex;
    const int nDataSize = sizeof(FILEMAP_POSHASHLINKMAP_ELEMENT);

    if (mem2file_getdata (hMem2File, nDataPos, pEle, nDataSize) < 0)
    {
        _error ("get data failed\n");
        return -1;
    }

    return 0;    
}

static int filemap_file_setposhashlinkitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, const FILEMAP_POSHASHLINKMAP_ELEMENT *pEle)
{
    if (nIndex < 0 || nIndex >= nMaxFileNum)
    {
        _error ("nIndex invalid, <%d,%d>\n", nIndex, nMaxFileNum);
        return -1;
    }

    /* 获取地图 */
    FILEMAP_GLOBAL_MAP sMap = {};
    if (filemap_getsegmap (nMaxFileNum, & sMap) < 0)
    {
        _error ("get map failed\n");
        return -1;
    }

    /* 得到待获取元素的位置 */
    const int nDataPos = sMap.seg_index.seg_hashlink.seg.pos + 
                    sizeof(FILEMAP_POSHASHLINKMAP_ELEMENT) * nIndex;
    const int nDataSize = sizeof(FILEMAP_POSHASHLINKMAP_ELEMENT);

    if (mem2file_setdata (hMem2File, nDataPos, pEle, nDataSize) < 0)
    {
        _error ("set data failed\n");
        return -1;
    }

    return 0;  
}

/**
 * @brief 获取数据段的元素
 */
static int filemap_file_getdatasegitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, FILEMAP_SECTION_DATA_ELEMENT *pElem)
{
    if (nIndex > nMaxFileNum || nIndex < 0)
    {
        _error ("nIndex invalid, <%d,%d>\n", nIndex, nMaxFileNum);
        return -1;
    }

    /* 获取地图 */
    FILEMAP_GLOBAL_MAP sMap = {};
    if (filemap_getsegmap (nMaxFileNum, & sMap) < 0)
    {
        _error ("get map failed\n");
        return -1;
    }

    const int nDataPos = sMap.seg_data.seg.pos + 
                    sizeof(FILEMAP_SECTION_DATA_ELEMENT) * nIndex;
    const int nDataSize = sizeof(FILEMAP_SECTION_DATA_ELEMENT);

    if (mem2file_getdata (hMem2File, nDataPos, pElem, nDataSize) < 0)
    {
        _error ("get data failed\n");
        return -1;
    }

    return 0;
}

/**
 * @brief 设置数据段的元素
 */
static int filemap_file_setdatasegitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, int nIndex, const FILEMAP_SECTION_DATA_ELEMENT *pElem)
{
    if (nIndex > nMaxFileNum || nIndex < 0)
    {
        _error ("nIndex invalid, <%d,%d>\n", nIndex, nMaxFileNum);
        return -1;
    }

    /* 获取地图 */
    FILEMAP_GLOBAL_MAP sMap = {};
    if (filemap_getsegmap (nMaxFileNum, & sMap) < 0)
    {
        _error ("get map failed\n");
        return -1;
    }

    const int nDataPos = sMap.seg_data.seg.pos + 
                    sizeof(FILEMAP_SECTION_DATA_ELEMENT) * nIndex;
    const int nDataSize = sizeof(FILEMAP_SECTION_DATA_ELEMENT);

    /* 检查是否需要扩展文件 */
    const int nFinalSize = nDataPos + nDataSize;

    int nFileSize = 0;
    if (mem2file_size (hMem2File, &nFileSize) < 0)
    {
        _error ("get size failed\n");
        return -1;
    }

    if (nFinalSize > nFileSize && nFinalSize <= sMap.seg.size)
    {
        if (mem2file_resize (hMem2File, nFinalSize) < 0)
        {
            _error ("resize failed\n");
            return -1;
        }
    }

    if (mem2file_setdata (hMem2File, nDataPos, pElem, nDataSize) < 0)
    {
        _error ("set data failed\n");
        return -1;
    }

    return 0;
}

/**
 * @brief 获取key对应的映射数据
 * @return 失败返回-1，找到返回1，没有找到返回0
 */
static int filemap_file_getdatamap(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key, FILEMAP_DATAMAP *pMap)
{
    int nHashIndex = filemap_hashmap_getindex (nMaxFileNum, key);

    FILEMAP_POSHASHMAP_ELEMENT sHashEle = {};
    if (filemap_file_getposhashmapitem(hMem2File, nMaxFileNum, nHashIndex, &sHashEle) < 0)
    {
        _error("get hashmap item failed\n");
        return -1;
    }

    if (! sHashEle.node.bUsedFlag)
    {
        return 0;
    }
    
    if (filemap_keycmp (& sHashEle.node.key, key) == 0)
    { /* 直接命中 */
        *pMap = sHashEle.node;
        return 1;
    }
    else 
    { /* 到链表中去找 */
        int nIndexNext = sHashEle.node.nNextIndex;

        while (1)
        {
            if (INDEX_NULL == nIndexNext)
            {
                break;
            }

            FILEMAP_POSHASHLINKMAP_ELEMENT sHashLinkEle = {};
            if (filemap_file_getposhashlinkitem (hMem2File, nMaxFileNum, nIndexNext, & sHashLinkEle) < 0)
            {
                _error ("get hashmap link item failed\n");
                return -1;
            }

            if (filemap_keycmp (& sHashLinkEle.node.key, key) == 0)
            { /* 在链表中命中 */
                *pMap = sHashLinkEle.node;
                return 1;
            }

            nIndexNext = sHashLinkEle.node.nNextIndex;
        }
    }

    return 0;
}

/**
 * @brief 添加key对应的映射数据，若已存在，则替换
 * @return 失败返回-1，成功返回1，已满返回0
 */
static int filemap_file_adddatamap(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_DATAMAP *map)
{
    /* 获取地图 */
    FILEMAP_GLOBAL_MAP sMap = {};
    if (filemap_getsegmap (nMaxFileNum, & sMap) < 0)
    {
        _error ("get map failed\n");
        return -1;
    }

    const int nPosHashLinkMap = sMap.seg_index.seg_bitmap_hashlink.seg.pos;
    const int nSizeHashLinkMap = sMap.seg_index.seg_bitmap_hashlink.seg.size;

    /* 获取元素在哈希表中的索引 */
    int nHashMapIndex = filemap_hashmap_getindex (nMaxFileNum, & (map->key));

    FILEMAP_POSHASHMAP_ELEMENT sHashEle = {};
    if (filemap_file_getposhashmapitem(hMem2File, nMaxFileNum, nHashMapIndex, &sHashEle) < 0)
    {
        _error("get hashmap item failed\n");
        return -1;
    }

    if (! sHashEle.node.bUsedFlag)
    { /* 直接找到了空位 */
        sHashEle.node = *map;
        sHashEle.node.bUsedFlag = 1;
        if (filemap_file_setposhashmapitem (hMem2File, nMaxFileNum, nHashMapIndex, &sHashEle) < 0)
        {
            _error ("set hashmap item failed\n");
            return -1;
        }

        return 1;
    }
    else 
    { /* 哈希表已有数据 */
        if (filemap_keycmp (& map->key, &sHashEle.node.key) == 0)
        { /* 哈希表为相同项，则直接替换 */
            sHashEle.node = *map;
            if (filemap_file_setposhashmapitem (hMem2File, nMaxFileNum, nHashMapIndex, &sHashEle) < 0)
            {
                _error ("set hashmap item failed\n");
                return -1;
            }
            return 1;
        }
        else 
        { /* 哈希表为不同项，则在链表中找 */
            if (INDEX_NULL == sHashEle.node.nNextIndex)
            { /* 如果只有一项：也即需要在链表中创建第一个项 */
                int nEmptyIndex = 0;
                if (filemap_file_scanfirstemptybit(hMem2File, nMaxFileNum,
                                                   nPosHashLinkMap, nSizeHashLinkMap, &nEmptyIndex) != 1)
                {
                    _error("get empty failed\n");
                    return -1;
                }

                FILEMAP_POSHASHLINKMAP_ELEMENT sHashLinkEleNew = {};
                sHashLinkEleNew.node.bUsedFlag = 1;
                sHashLinkEleNew.node.key = map->key;
                sHashLinkEleNew.node.nIndex = map->nIndex;
                sHashLinkEleNew.node.nNextIndex = INDEX_NULL;

                FILEMAP_POSHASHMAP_ELEMENT sHashLinkMod = sHashEle;
                sHashLinkMod.node.nNextIndex = nEmptyIndex;

                if (1 || "union operation")
                {
                    if (filemap_file_setposhashmapitem(hMem2File, nMaxFileNum, nHashMapIndex, &sHashLinkMod) < 0)
                    { /* 调整本项 */
                        _error("set pos hash link item failed\n");
                        return -1;
                    }

                    if (filemap_file_setposhashlinkitem(hMem2File, nMaxFileNum, nEmptyIndex, &sHashLinkEleNew) < 0)
                    { /* 加入下一项 */
                        _error("set hashlink item failed\n");
                        return -1;
                    }

                    if (filemap_file_setbitmap(hMem2File, nMaxFileNum, nPosHashLinkMap, nSizeHashLinkMap,
                                               nEmptyIndex, 1) < 0)
                    { /* 记录下一项 */
                        _error("set hashlink bit failed\n");
                        return -1;
                    }
                }
            }
            else 
            { /* 如果不只有一项：也即链表已存在第一项 */
                int nIndexNext = sHashEle.node.nNextIndex; /* 链表的第一项索引 */

                while (1)
                {
                    FILEMAP_POSHASHLINKMAP_ELEMENT sHashLinkEle = {};
                    if (filemap_file_getposhashlinkitem(hMem2File, nMaxFileNum, nIndexNext, &sHashLinkEle) < 0)
                    {
                        _error("get hashmap link item failed\n");
                        return -1;
                    }

                    if (filemap_keycmp(&sHashLinkEle.node.key, &map->key) == 0)
                    { /* 在链表中命中 */
                        sHashLinkEle.node = *map;
                        if (filemap_file_setposhashlinkitem(hMem2File, nMaxFileNum, nIndexNext, &sHashLinkEle) < 0)
                        {
                            _error("set hashmap link failed\n");
                            return -1;
                        }
                        return 1;
                    }

                    int nIndexPrev = nIndexNext;    /* 记录当前项 */ 
                    nIndexNext = sHashLinkEle.node.nNextIndex; /* 指向下一项 */

                    if (INDEX_NULL == nIndexNext)
                    { /* 如果链表中不存在，则需要新增 */


                        int nEmptyIndex = 0;
                        if (filemap_file_scanfirstemptybit(hMem2File, nMaxFileNum,
                                                           nPosHashLinkMap, nSizeHashLinkMap, &nEmptyIndex) != 1)
                        {
                            _error("get empty failed\n");
                            return -1;
                        }

                        FILEMAP_POSHASHLINKMAP_ELEMENT sHashLinkEleNew = sHashLinkEle;
                        sHashLinkEleNew.node.bUsedFlag = 1;
                        sHashLinkEleNew.node.key = map->key;
                        sHashLinkEleNew.node.nIndex = map->nIndex;
                        sHashLinkEleNew.node.nNextIndex = INDEX_NULL;

                        FILEMAP_POSHASHLINKMAP_ELEMENT sHashLinkEleMod = sHashLinkEle;
                        sHashLinkEleMod.node.nNextIndex = nEmptyIndex;

                        /* 以下为联合操作，若出错，则会引起一致性问题 */
                        if (1 || "union operation")
                        {
                            if (filemap_file_setposhashlinkitem (hMem2File, nMaxFileNum, nIndexPrev, &sHashLinkEleMod) < 0)
                            { /* 调整本项 */
                                _error ("set pos hash link item failed\n");
                                return -1;
                            }

                            if (filemap_file_setposhashlinkitem(hMem2File, nMaxFileNum, nEmptyIndex, &sHashLinkEleNew) < 0)
                            { /* 加入下一项 */
                                _error("set hashlink item failed\n");
                                return -1;
                            }
                            if (filemap_file_setbitmap(hMem2File, nMaxFileNum, nPosHashLinkMap, nSizeHashLinkMap,
                                                       nEmptyIndex, 1) < 0)
                            { /* 记录下一项 */
                                _error("set hashlink bit failed\n");
                                return -1;
                            }
                        }
                        return 1;
                    }
                }
            }


            return 0;
        }
    }
    return 0;
}

/**
 * @brief 删除key对应的映射数据
 * @return 失败返回-1，成功返回0
 */
static int filemap_file_deldatamap(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key)
{
    /* 获取地图 */
    FILEMAP_GLOBAL_MAP sMap = {};
    if (filemap_getsegmap (nMaxFileNum, & sMap) < 0)
    {
        _error ("get map failed\n");
        return -1;
    }

    const int nPosHashLinkMap = sMap.seg_index.seg_bitmap_hashlink.seg.pos;
    const int nSizeHashLinkMap = sMap.seg_index.seg_bitmap_hashlink.seg.size;
    const int nPosDataBitMap = sMap.seg_index.seg_bitmap_data.seg.pos;
    const int nSizeDataBitMap = sMap.seg_index.seg_bitmap_data.seg.size;

    /* 获取元素在哈希表中的索引 */
    const int nHashMapIndex = filemap_hashmap_getindex (nMaxFileNum, key);

    FILEMAP_POSHASHMAP_ELEMENT sHashEle = {};
    if (filemap_file_getposhashmapitem(hMem2File, nMaxFileNum, nHashMapIndex, &sHashEle) < 0)
    {
        _error("get hashmap item failed\n");
        return -1;
    }

    if (! sHashEle.node.bUsedFlag)
    { /* 要删除的映射不存在 */
        _error ("key not exist\n");
        return -1;
    }
    else 
    { /* 在哈希表中存在 */
        if (filemap_keycmp (key, &sHashEle.node.key) == 0)
        { /* 哈希表为相同项，则删除该项 */

            if (INDEX_NULL != sHashEle.node.nNextIndex)
            { /* 如果存在后继节点 */
                /* 取出一个节点，并替换掉当前节点即可 */
                FILEMAP_POSHASHLINKMAP_ELEMENT sHashLinkEle = {};
                if (filemap_file_getposhashlinkitem (hMem2File, nMaxFileNum, sHashEle.node.nNextIndex, &sHashLinkEle) < 0)
                {
                    _error ("get item failed\n");
                    return -1;
                }

                FILEMAP_POSHASHMAP_ELEMENT sHashEleTmp = {};
                sHashEleTmp.node = sHashLinkEle.node;
                sHashEleTmp.node.bUsedFlag = 1;

                if (1 || "union operation")
                {
                    if (filemap_file_setposhashmapitem(hMem2File, nMaxFileNum, nHashMapIndex, &sHashEleTmp) < 0)
                    { /* 将当前节点替换位下一个节点 */
                        _error ("set item failed\n");
                        return -1;
                    }
                    if (filemap_file_setbitmap (hMem2File, nMaxFileNum, nPosDataBitMap, nSizeDataBitMap,
                                sHashEle.node.nIndex, 0) < 0)
                    { /* 将数据标记位删除 */
                        _error ("set bit failed\n");
                        return -1;
                    }
                    if (filemap_file_setbitmap (hMem2File, nMaxFileNum, nPosHashLinkMap, nSizeHashLinkMap, 
                                sHashEle.node.nNextIndex, 0) < 0)
                    { /* 下一个节点标记为删除 */
                        _error ("set bit failed\n");
                        return -1;
                    }
                }
            }
            else 
            { /* 如果不存在后继节点，则直接删除即可 */
                sHashEle.node.bUsedFlag = 0;

                if (1 || "union operation")
                {
                    if (filemap_file_setposhashmapitem(hMem2File, nMaxFileNum, nHashMapIndex, &sHashEle) < 0)
                    { /* 将当前节点置为无效 */
                        _error("set hashmap item failed\n");
                        return -1;
                    }
                    if (filemap_file_setbitmap(hMem2File, nMaxFileNum, nPosDataBitMap, nSizeDataBitMap,
                                               sHashEle.node.nIndex, 0) < 0)
                    { /* 将数据标记位删除 */
                        _error("set bit failed\n");
                        return -1;
                    }
                }
                return 0;
            }

            return 0;
        }
        else 
        { /* 哈希表中不存在，则去链表中删除 */
            int nIndexNext = sHashEle.node.nNextIndex; /* 链表的第一项 */

            if (INDEX_NULL == nIndexNext)
            { /* 链表是空的 */
                _error ("del item, item not exist\n");
                return -1;
            }
            else 
            {
                int nIndexPrevItem = INDEX_NULL;
                while (1)
                {
                    FILEMAP_POSHASHLINKMAP_ELEMENT sEleHashLinkEle = {};
                    if (filemap_file_getposhashlinkitem (hMem2File, nMaxFileNum, nIndexNext, &sEleHashLinkEle) < 0)
                    {
                        _error ("get item failed\n");
                        return -1;
                    }

                    if (filemap_keycmp(&sEleHashLinkEle.node.key, key) == 0)
                    { /* 找到了这一项 */
                        if (nIndexNext == sHashEle.node.nNextIndex)
                        { /* 如果是链表的第一项 */
                            FILEMAP_POSHASHMAP_ELEMENT sHashMapEleMod = sHashEle;
                            sHashMapEleMod.node.nNextIndex = sEleHashLinkEle.node.nNextIndex; /* 指向下一个节点 */
                            
                            if (1 || "union operation")
                            {
                                if (filemap_file_setposhashmapitem (hMem2File, nMaxFileNum, nHashMapIndex,
                                        & sHashMapEleMod) < 0)
                                { /* 修改哈希表中的节点 */
                                    _error ("set hash map item failed\n");
                                    return -1;
                                }

                                if (filemap_file_setbitmap(hMem2File, nMaxFileNum, nPosHashLinkMap, nSizeHashLinkMap,
                                                           nIndexNext, 0) < 0)
                                { /* 删除这一项的记录 */
                                    _error("set hashlink bit failed\n");
                                    return -1;
                                }

                                if (filemap_file_setbitmap(hMem2File, nMaxFileNum, nPosDataBitMap, nSizeDataBitMap,
                                                           sEleHashLinkEle.node.nIndex, 0) < 0)
                                { /* 将数据标记位删除 */
                                    _error("set bit failed\n");
                                    return -1;
                                }
                            }

                            return 0;
                        }
                        else
                        { /* 如果不是链表的第一项 */
                            FILEMAP_POSHASHLINKMAP_ELEMENT sEleHashLinkElePrev = {};
                            if (filemap_file_getposhashlinkitem(hMem2File, nMaxFileNum, nIndexPrevItem, &sEleHashLinkElePrev) < 0)
                            { /* 找到上一项 */
                                _error("get item failed\n");
                                return -1;
                            }
                            sEleHashLinkElePrev.node.nNextIndex = sEleHashLinkEle.node.nNextIndex; /* 指向下一项 */

                            if (1 || "union operation")
                            {
                                if (filemap_file_setposhashlinkitem (hMem2File, nMaxFileNum, nIndexPrevItem,
                                        & sEleHashLinkElePrev) < 0)
                                { /* 修改上一项 */
                                    _error ("set hash map item failed\n");
                                    return -1;
                                }

                                if (filemap_file_setbitmap(hMem2File, nMaxFileNum, nPosHashLinkMap, nSizeHashLinkMap,
                                                           nIndexNext, 0) < 0)
                                { /* 删除这一项的记录 */
                                    _error("set hashlink bit failed\n");
                                    return -1;
                                }

                                if (filemap_file_setbitmap(hMem2File, nMaxFileNum, nPosDataBitMap, nSizeDataBitMap,
                                                           sEleHashLinkEle.node.nIndex, 0) < 0)
                                { /* 将数据标记位删除 */
                                    _error("set bit failed\n");
                                    return -1;
                                }
                            }

                            return 0;
                        }
                    }

                    nIndexPrevItem = nIndexNext; /* 记录上一个循环的项 */
                    nIndexNext = sEleHashLinkEle.node.nNextIndex; /* 指向下一项 */

                    if (INDEX_NULL == nIndexNext)
                    {
                        _error ("delete item, item not found\n");
                        return -1;
                    }
                }
            }
        }
    }

    return -1;
}

/**
 * @brief 根据key计算出项在位置哈希表中的索引值
 */
static int filemap_hashmap_getindex (int nMaxFileNum, const FILEMAP_KEY *key)
{
    const int nHashMapSize = filemap_get_poshashmap_num (nMaxFileNum);

    /* 根据key得到索引 */
    unsigned int uIndex = BKDRHash (key->szKey); /* 这里要用无符号型，进行取整 */
    uIndex %= nHashMapSize;

    return uIndex;
}

/**
 * @return 若存在，返回1，否则返回0
 */
static int filemap_file_existitem (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key)
{
    int bExist = 0;

    FILEMAP_DATAMAP map = {};
    int ret = filemap_file_getdatamap (hMem2File, nMaxFileNum, key, & map);

    if (ret <= 0)
    { /* 失败也认为是不存在 */
        bExist = 0;
    }
    else 
    {
        bExist = 1;
    }

    return bExist;
}

static int filemap_keycmp (const FILEMAP_KEY *keyA, const FILEMAP_KEY *keyB)
{
    return strcmp (keyA->szKey, keyB->szKey);
}

static int filemap_getdefsegmap (FILEMAP_DEF_MAP *psMap)
{
    psMap->seg.pos = 0;
    psMap->seg.size = 10 * 1024;

    return 0;
}

static int filemap_file_getitem(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key, FILEMAP_VALUE *value)
{
    FILEMAP_DATAMAP map = {};
    int ret = filemap_file_getdatamap (hMem2File, nMaxFileNum, key, & map);
    if (ret < 0)
    {
        _error ("get data index failed\n");
        return -1;
    }
    else if (ret == 0)
    {
        return -1;
    }
    else 
    {
        FILEMAP_SECTION_DATA_ELEMENT sDataElem = {};
        ret = filemap_file_getdatasegitem (hMem2File, nMaxFileNum, map.nIndex, & sDataElem);
        if (ret < 0)
        {
            _error ("get data element failed\n");
            return -1;
        }

        *value = sDataElem.value;
    }

    return 0;
}

/**
 * @brief 记录一个项，若存在，则替换，若不存在，则新增
 * @return 成功返回1，出错返回-1，已满返回0
 */
static int filemap_file_setitem(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key, const FILEMAP_VALUE *value)
{
    /**
     * 如果该项存在，则替换，否则
     * 找出要放的位置，然后记录索引
     */

    /* 获取地图 */
    FILEMAP_GLOBAL_MAP sMap = {};
    if (filemap_getsegmap (nMaxFileNum, & sMap) < 0)
    {
        _error ("get map failed\n");
        return -1;
    }    

    const int nPosBitmapData = sMap.seg_index.seg_bitmap_data.seg.pos;
    const int nSizeBitmapData = sMap.seg_index.seg_bitmap_data.seg.size;

    FILEMAP_DATAMAP sDataMap = {};
    int ret = filemap_file_getdatamap (hMem2File, nMaxFileNum, key, & sDataMap);

    int bAddNew = 0;

    if (ret < 0)
    {
        _error ("getdatamap failed\n");
        return -1;
    }
    else if (ret == 1)
    { /* 存在 */
        FILEMAP_SECTION_DATA_ELEMENT sEle = {
            *value,
        };
        ret = filemap_file_setdatasegitem(hMem2File, nMaxFileNum,
                                            sDataMap.nIndex, &sEle);
        if (ret < 0)
        {
            _error("set data to segitem failed\n");
            return -1;
        }
        return 1;
    }
    else if (ret == 0)
    { /* 不存在，则需要新增映射和数据 */
        bAddNew = 1;
    }

    if (bAddNew)
    {
        /* 先填充数据 */
        int nEmptyDataIndex = 0;
        if (filemap_file_scanfirstemptybit (hMem2File, nMaxFileNum, nPosBitmapData, nSizeBitmapData, &nEmptyDataIndex) != 1)
        {
            _error ("scan empty bit failed\n");
            return -1;
        }

        FILEMAP_SECTION_DATA_ELEMENT sDataEle = {
            *value,
        };

        if (filemap_file_setdatasegitem (hMem2File, nMaxFileNum, nEmptyDataIndex, &sDataEle) < 0)
        {
            _error ("set seg item failed\n");
            return -1;
        }

        if (1 || "union operation")
        {
            /* 设置数据标志位 */
            if (filemap_file_setbitmap (hMem2File, nMaxFileNum, nPosBitmapData, nSizeBitmapData, 
                        nEmptyDataIndex, 1) < 0)
            {
                _error ("set data seg bit failed\n");
                return -1;
            }
            
            /* 增加索引 */
            FILEMAP_DATAMAP sNewMap = {};
            sNewMap.bUsedFlag = 1;
            sNewMap.key = *key;
            sNewMap.nIndex = nEmptyDataIndex;
            sNewMap.nNextIndex = INDEX_NULL;
            if (filemap_file_adddatamap (hMem2File, nMaxFileNum, & sNewMap) < 0)
            {
                _error ("set new map failed\n");
                return -1;
            }
        }

        return 1;
    }

    return -1;
}

/**
 * @brief 删除一项
 * @return 成功返回1，元素不存在返回0，失败返回-1
 */
static int filemap_file_deleteitem(MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const FILEMAP_KEY *key)
{
    /**
     * 如果该项存在，则替换，否则
     * 找出要放的位置，然后记录索引
     */

    /* 获取地图 */
    FILEMAP_GLOBAL_MAP sMap = {};
    if (filemap_getsegmap (nMaxFileNum, & sMap) < 0)
    {
        _error ("get map failed\n");
        return -1;
    }    

    FILEMAP_DATAMAP sDataMap = {};
    int ret = filemap_file_getdatamap (hMem2File, nMaxFileNum, key, & sDataMap);

    if (ret < 0)
    {
        _error ("get data map failed\n");
        return -1;
    }
    else if (ret == 0)
    {
        _error ("not exist\n");
        return 0;
    }
    else if (ret == 1)
    {
        if (1 || "union operation")
        {
            if (filemap_file_deldatamap (hMem2File, nMaxFileNum, key) < 0)
            {
                _error ("delete data map failed\n");
                return -1;
            }
        }
        return 1;
    }

    return -1;
}

static int filemap_entrancecall_lock (FILEMAP_HANDLE hInstance)
{
    FILEMAP_OBJ *pObj = (FILEMAP_OBJ*)hInstance;
    int ret = pthread_mutex_lock (& pObj->mutex_entrance_call);
    if (ret != 0)
    {
        _error ("lock failed\n");
        return -1;
    }

    return 0;
}

static int filemap_entrancecall_unlock (FILEMAP_HANDLE hInstance)
{
    FILEMAP_OBJ *pObj = (FILEMAP_OBJ*)hInstance;
    int ret = pthread_mutex_unlock (& pObj->mutex_entrance_call);
    if (ret != 0)
    {
        _error ("lock failed\n");
        return -1;
    }

    return 0;
}

static int filemap_file_generateinfo (MEM2FILE_HANDLE hMem2File, int nMaxFileNum, const char *szFileName)
{
#ifndef DEBUG
    return 0;
#endif 

    FILE *fp = fopen (szFileName, "w");

    if (NULL == fp)
    {
        _error ("open <%s> failed\n", szFileName);
        return -1;
    }

    if (1)
    {
        fprintf(fp, "filemapinfo: \n");
        fprintf (fp, "{\n");
        fprintf (fp, "  obj=%p\n", hMem2File);
        fprintf (fp, "}\n\n");
    }   
    
    FILEMAP_GLOBAL_MAP sMap = {};
    filemap_getsegmap (nMaxFileNum, &sMap);

    if (1)
    { /* 文件基本信息 */
        fprintf (fp, "fileinfo: \n");
        fprintf (fp, "{\n");

        int nFileSize = 0;
        int ret_size = mem2file_size (hMem2File, & nFileSize);
        fprintf (fp, "  file size=%d,ret=%d\n", nFileSize, ret_size);

        FILEMAP_SECTION_DEF sDefSec = {};
        int ret_getdefseg = filemap_get_defseg (hMem2File, & sDefSec);
        fprintf (fp, "  version=<%s>,maxfilenum=%d,ret=%d\n",
                        sDefSec.szVersion, sDefSec.nMaxFileNum, ret_getdefseg);

        fprintf (fp, "}\n\n");
    }

    if (1)
    { /* 分区地图 */
        fprintf (fp, "global_map:\n");
        fprintf (fp, "{\n");
        fprintf (fp, "  seg:[pos=%d,size=%d]\n", sMap.seg.pos, sMap.seg.size);
        fprintf (fp, "  seg_def:\n");
        fprintf (fp, "  {\n");
        fprintf (fp, "    seg: [pos=%d,size=%d]\n",
                        sMap.seg_def.seg.pos, 
                        sMap.seg_def.seg.size);
        fprintf (fp, "  }\n");
        fprintf (fp, "  seg_index: \n");
        fprintf (fp, "  {\n");
        fprintf (fp, "    seg_bitmap_data:\n");
        fprintf (fp, "    {\n");
        fprintf (fp, "      seg: [pos=%d,size=%d]\n", 
                        sMap.seg_index.seg_bitmap_data.seg.pos,
                        sMap.seg_index.seg_bitmap_data.seg.size);
        fprintf (fp, "    }\n");
        fprintf (fp, "    seg_bitmap_hashlink:\n");
        fprintf (fp, "    {\n");
        fprintf (fp, "      seg: [pos=%d,size=%d]\n", 
                        sMap.seg_index.seg_bitmap_hashlink.seg.pos,
                        sMap.seg_index.seg_bitmap_hashlink.seg.size);
        fprintf (fp, "    }\n");
        fprintf (fp, "    seg_hashmap:\n");
        fprintf (fp, "    {\n");
        fprintf (fp, "      seg: [pos=%d,size=%d]\n",
                        sMap.seg_index.seg_hashmap.seg.pos,
                        sMap.seg_index.seg_hashmap.seg.size);
        fprintf (fp, "    }\n");
        fprintf (fp, "    seg_hashlink:\n");
        fprintf (fp, "    {\n");
        fprintf (fp, "       seg: [pos=%d,size=%d]\n",
                        sMap.seg_index.seg_hashlink.seg.pos,
                        sMap.seg_index.seg_hashlink.seg.size);
        fprintf (fp, "    }\n");
        fprintf (fp, "  }\n");
        fprintf (fp, "  seg_data:\n");
        fprintf (fp, "  {\n");
        fprintf (fp, "    seg: [pos=%d,size=%d]\n", 
                        sMap.seg_data.seg.pos,
                        sMap.seg_data.seg.size);
        fprintf (fp, "  }\n");
        fprintf (fp, "}\n\n");
    }

    if (1)
    { /* 数据位信息 */
        fprintf (fp, "bitmap_data: \n");
        fprintf (fp, "{\n");

        const int nBitmapPos = sMap.seg_index.seg_bitmap_data.seg.pos;
        const int nBitmapSize = sMap.seg_index.seg_bitmap_data.seg.size;

        for (int k = 0; ; ++k)
        {
            char byteBuffer[256] = {};

            const int nReadPos = nBitmapPos + sizeof(byteBuffer) * k;
            if ( (nReadPos - nBitmapPos) > nBitmapSize)
            {
                _debug ("scan finished\n");
                break;
            }

            const int nLeftSize = nBitmapSize - (nReadPos - nBitmapPos);
            const int nReadSize = (nLeftSize > sizeof(byteBuffer) ? 
                                        sizeof(byteBuffer) : nLeftSize);

            int ret = mem2file_getdata(hMem2File, nReadPos, byteBuffer, nReadSize);
            if (ret < 0)
            {
                _error ("get data failed\n");
            }

            for (int i = 0; i < nReadSize; ++i)
            {
                if ((i % 10) == 0)
                {
                    fprintf(fp, "  [%d-%d] ", i, i + 10);
                }
                int cTmp = (unsigned char)byteBuffer[i];

                fprintf(fp, "%02X ", cTmp);

                if ((i + 1) % 10 == 0)
                {
                    fprintf(fp, "\n");
                }
            }

            fprintf (fp, "\n}\n\n");
        }
    }

    if (1)
    { /* 数据链表位信息 */
        fprintf (fp, "bitmap_hashlink: \n");
        fprintf (fp, "{\n");

        const int nBitmapPos = sMap.seg_index.seg_bitmap_hashlink.seg.pos;
        const int nBitmapSize = sMap.seg_index.seg_bitmap_hashlink.seg.size;

        for (int k = 0; ; ++k)
        {
            char byteBuffer[256] = {};

            const int nReadPos = nBitmapPos + sizeof(byteBuffer) * k;
            if ( (nReadPos - nBitmapPos) > nBitmapSize)
            {
                _debug ("scan finished\n");
                break;
            }

            const int nLeftSize = nBitmapSize - (nReadPos - nBitmapPos);
            const int nReadSize = (nLeftSize > sizeof(byteBuffer) ? 
                                        sizeof(byteBuffer) : nLeftSize);

            int ret = mem2file_getdata(hMem2File, nReadPos, byteBuffer, nReadSize);
            if (ret < 0)
            {
                _error ("get data failed\n");
            }

            for (int i = 0; i < nReadSize; ++i)
            {
                if ((i % 10) == 0)
                {
                    fprintf(fp, "  [%d-%d] ", i, i + 10);
                }
                int cTmp = (unsigned char)byteBuffer[i];

                fprintf(fp, "%02X ", cTmp);

                if ((i + 1) % 10 == 0)
                {
                    fprintf(fp, "\n");
                }
            }

            fprintf (fp, "\n}\n\n");
        }
    }

    if (1)
    { /* 哈希映射表 */
        fprintf (fp, "poshashmap:\n");
        fprintf (fp, "{\n");

        const int nMaxFileNumEx = filemap_get_poshashmap_num (nMaxFileNum);

        for (int i = 0; i < nMaxFileNumEx; ++i)
        {
            FILEMAP_POSHASHMAP_ELEMENT sEle = {};
            int ret = filemap_file_getposhashmapitem (hMem2File, nMaxFileNum, i, & sEle);
            if (ret < 0)
            {
                _error ("get poshashmap item failed\n");
            }

            int nHashValue = filemap_hashmap_getindex (nMaxFileNum, & (sEle.node.key));

            fprintf (fp, "  [%d] used_flag=%d,key=%s,hash=%d,index=%d,next=%d,ret=%d\n",
                i, sEle.node.bUsedFlag, sEle.node.key.szKey, nHashValue, sEle.node.nIndex,
                sEle.node.nNextIndex, ret);
        }

        fprintf (fp, "}\n\n");
    }

    if (1)
    { /* 位置哈希链映射表 */
        fprintf(fp, "poshashlinkmap:\n");
        fprintf (fp, "{\n");

        for (int i = 0; i < nMaxFileNum; ++i)
        {
            FILEMAP_POSHASHLINKMAP_ELEMENT sEle = {};
            int ret = filemap_file_getposhashlinkitem (hMem2File, nMaxFileNum, i, & sEle);
            if (ret < 0)
            {
                _error ("get pos hash link item failed\n");
            }

            int nHashValue = filemap_hashmap_getindex (nMaxFileNum, & (sEle.node.key));

            fprintf (fp, "  [%d] used_flag=%d,key=%s,hash=%d,index=%d,next=%d,ret=%d\n",
                i, sEle.node.bUsedFlag, sEle.node.key.szKey, nHashValue, sEle.node.nIndex,
                sEle.node.nNextIndex, ret);
        }

        fprintf (fp, "}\n\n");
    }

    if (1)
    { /* 数据段 */
        fprintf (fp, "dataseg:\n");
        fprintf (fp, "{\n");

        for (int i = 0; i < nMaxFileNum; ++i)
        {
            FILEMAP_SECTION_DATA_ELEMENT sEle = {};
            int ret = filemap_file_getdatasegitem (hMem2File, nMaxFileNum, i, & sEle);
            if (ret < 0)
            {
                _error ("get data at [%d] fail, break\n", i);
                break;
            }
            char value[32] = {};
            strncpy (value, sEle.value.byteData, sizeof(value) - 1);
            fprintf (fp, "  [%d] value=%s...\n", i, value);
        }

        fprintf (fp, "}\n");
    }

    fclose(fp);
    return 0;
}

static int filemap_getsegmap (int nMaxFileNum, FILEMAP_GLOBAL_MAP *psMap)
{
    int nPosTmp = 0;

    /* 整体 */
    psMap->seg.pos = nPosTmp;

    /* 定义段 */
    filemap_getdefsegmap (& psMap->seg_def);

    nPosTmp = psMap->seg_def.seg.size + psMap->seg_def.seg.pos;

    /* 索引段 */
    psMap->seg_index.seg.pos = nPosTmp;

    /* 索引-数据段比特表 */
    psMap->seg_index.seg_bitmap_data.seg.pos = nPosTmp;
    psMap->seg_index.seg_bitmap_data.seg.size = nMaxFileNum / 8 + (nMaxFileNum % 8 ? 1 : 0);

    nPosTmp += psMap->seg_index.seg_bitmap_data.seg.size;

    /* 索引-位置链表比特表 */
    psMap->seg_index.seg_bitmap_hashlink.seg.pos = nPosTmp;
    psMap->seg_index.seg_bitmap_hashlink.seg.size = nMaxFileNum / 8 + (nMaxFileNum % 8 ? 1 : 0);

    nPosTmp += psMap->seg_index.seg_bitmap_hashlink.seg.size;

    /* 索引-位置哈希表 */
    psMap->seg_index.seg_hashmap.seg.pos = nPosTmp;
    psMap->seg_index.seg_hashmap.seg.size = filemap_get_poshashmap_num (nMaxFileNum) *
                    sizeof(FILEMAP_POSHASHMAP_ELEMENT);
    
    nPosTmp += psMap->seg_index.seg_hashmap.seg.size;

    /* 索引-位置哈希链表 */
    psMap->seg_index.seg_hashlink.seg.pos = nPosTmp;
    psMap->seg_index.seg_hashlink.seg.size = nMaxFileNum * 
                    sizeof(FILEMAP_POSHASHLINKMAP_ELEMENT);
    
    nPosTmp += psMap->seg_index.seg_hashlink.seg.size;

    /* 索引段整体 */
    psMap->seg_index.seg.size = psMap->seg_index.seg_bitmap_data.seg.size + 
                    psMap->seg_index.seg_bitmap_hashlink.seg.size +
                    psMap->seg_index.seg_hashmap.seg.size +
                    psMap->seg_index.seg_hashlink.seg.size;

    /* 数据段 */
    psMap->seg_data.seg.pos = nPosTmp;
    psMap->seg_data.seg.size = nMaxFileNum * sizeof(FILEMAP_SECTION_DATA_ELEMENT);

    /* 整体 */
    psMap->seg.size = psMap->seg_def.seg.size +
                    psMap->seg_index.seg.size +
                    psMap->seg_data.seg.size;

    return 0;
}

/************ GLOBAL FUNCS ************/

/**
 * 单进单出
 */
FILEMAP_HANDLE filemap_create (const char *szFileName, int nNum)
{
    int bError = 0;

    FILEMAP_HANDLE hFileMap = NULL;
    if (0 == bError)
    {
        hFileMap = filemap_init_file (szFileName, nNum); 
        if (NULL == hFileMap)
        {
            _error ("init file failed\n");
            bError = 1;
        }
    }

    return hFileMap;
}

FILEMAP_HANDLE filemap_load (const char *szFileName)
{
    int bError = 0;

    FILEMAP_HANDLE hFileMap = NULL;
    if (0 == bError)
    {
        hFileMap = filemap_init_file (szFileName, -1); 
        if (NULL == hFileMap)
        {
            _error ("load file failed\n");
            bError = 1;
        }
    }

    return hFileMap;
}

int filemap_close (FILEMAP_HANDLE hInstance)
{
    filemap_entrancecall_lock (hInstance);
    int ret = filemap_close_file (hInstance);
    filemap_entrancecall_unlock (hInstance);

    return ret;
}

int filemap_existitem (FILEMAP_HANDLE hInstance, const FILEMAP_KEY *key)
{
    FILEMAP_OBJ *pObj = (FILEMAP_OBJ*)hInstance;

    filemap_entrancecall_lock (hInstance);
    int ret = filemap_file_existitem (pObj->hMem2File, pObj->nMaxFileNum, key);
    filemap_entrancecall_unlock (hInstance);

    return ret;
}

int filemap_getitem (FILEMAP_HANDLE hInstance, const FILEMAP_KEY *key, FILEMAP_VALUE *value)
{
    FILEMAP_OBJ *pObj = (FILEMAP_OBJ*)hInstance;

    filemap_entrancecall_lock (hInstance);
    int ret = filemap_file_getitem (pObj->hMem2File, pObj->nMaxFileNum, key, value);
    filemap_entrancecall_unlock (hInstance);

    return ret;
}

int filemap_setitem (FILEMAP_HANDLE hInstance, const FILEMAP_KEY *key, const FILEMAP_VALUE *value)
{
    FILEMAP_OBJ *pObj = (FILEMAP_OBJ*) hInstance;

    filemap_entrancecall_lock (hInstance);
    int ret = filemap_file_setitem (pObj->hMem2File, pObj->nMaxFileNum, key, value);
    ret = (ret == 1 ? 0 : -1);
    filemap_entrancecall_unlock (hInstance);

    return ret;
}

int filemap_deleteitem (FILEMAP_HANDLE hInstance, const FILEMAP_KEY *key)
{
    FILEMAP_OBJ *pObj = (FILEMAP_OBJ*) hInstance;

    filemap_entrancecall_lock (hInstance);
    int ret = filemap_file_deleteitem (pObj->hMem2File, pObj->nMaxFileNum, key);
    ret = (ret == 1 ? 0 : -1);
    filemap_entrancecall_unlock (hInstance);

    return ret;
}

int filemap_generateinfo (FILEMAP_HANDLE hInstance, const char *szFileName)
{
    FILEMAP_OBJ *pObj = (FILEMAP_OBJ*) hInstance;

    filemap_entrancecall_lock (hInstance);
    int ret = filemap_file_generateinfo (pObj->hMem2File, pObj->nMaxFileNum, szFileName);
    filemap_entrancecall_unlock (hInstance);

    return ret;
}