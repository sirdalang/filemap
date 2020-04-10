
#include "test.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <math.h>

#include <map>
#include <string>
#include <list>
#include <set>

#define DEBUG

#ifdef DEBUG
#define _debug(x...) do {printf("[test][debug][%s %d %s]", \
	__FILE__,__LINE__,__FUNCTION__);printf(x);} while (0)
#define _info(x...) do {printf("[test][info][%s %d %s]", \
	__FILE__,__LINE__,__FUNCTION__);printf(x);} while (0)
#define _error(x...) do {printf("[test][error][%s %d %s]", \
	__FILE__,__LINE__,__FUNCTION__);printf(x);} while (0)
#else 
#define _debug(x...) do {;} while (0)
#define _info(x...) do {printf("[test][info][%s %d %s]", \
	__FILE__,__LINE__,__FUNCTION__);printf(x);} while (0)
#define _error(x...) do {printf("[test][error][%s %d %s]", \
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

// #include <filemap.h>
#include "../filemap.h"

/**
 * 对合法的操作进行测试
 * 
 */
int test_filemap_normal (int nTotalNum, int nTestNum, const char *szFileName)
{
    _debug ("testing: [%d,%d,%s]\n", nTotalNum, nTestNum, szFileName);

    char szObjFile[64] = {};
    snprintf (szObjFile, sizeof(szObjFile), "test.dat_normal_%d_%d", nTotalNum, nTestNum);

    FILEMAP_HANDLE hFileMap = filemap_create (szObjFile, nTotalNum);

    assert (hFileMap != NULL);

    std::map<std::string, std::string> cMap;

    for (int i = 0; i < nTestNum; ++i)
    {
        char szName[64] = {};
        char szValue[64] = {};
        snprintf (szName, sizeof(szName), "name%d_%d", i, i*i);
        snprintf (szValue, sizeof(szValue), "value%d_%d", i, i*i); /* 这样更容易碰撞 */
        cMap[szName] = szValue;
    }

    /* 添加 */
    for (auto it = cMap.cbegin(); it != cMap.cend(); ++it)
    {
        FILEMAP_KEY key = {};
        FILEMAP_VALUE value = {};
        snprintf (key.szKey, sizeof(key.szKey), "%s", it->first.data());
        snprintf (value.byteData, sizeof(value.byteData), "%s", it->second.data());
        int ret = filemap_setitem (hFileMap, &key, &value);
        assert (ret == 0);
    }

    /* 查询 */
    for (auto it = cMap.cbegin(); it != cMap.cend(); ++it)
    {
        FILEMAP_KEY key = {};
        FILEMAP_VALUE value = {};
        snprintf (key.szKey, sizeof(key.szKey), "%s", it->first.data());

        int ret = filemap_getitem (hFileMap, &key, &value);
        assert (ret == 0);
        if (it->second != value.byteData)
        {
            _error ("not equal, a=%s,b=%s\n", it->second.data(), value.byteData);
        }
        assert (it->second == value.byteData);
    }

    std::map<std::string, std::string> cMapMod;
    for (int i = 0; i < nTestNum; ++i)
    {
        char szName[64] = {};
        char szValue[64] = {};
        snprintf (szName, sizeof(szName), "name%d_%d", i, i*i);
        snprintf (szValue, sizeof(szValue), "value%d", i*i*i);
        cMapMod[szName] = szValue;
    }

    /* 修改 */
    for (auto it = cMapMod.cbegin(); it != cMapMod.cend(); ++it)
    {
        FILEMAP_KEY key = {};
        FILEMAP_VALUE value = {};
        snprintf (key.szKey, sizeof(key.szKey), "%s", it->first.data());
        snprintf (value.byteData, sizeof(value.byteData), "%s", it->second.data());
        int ret = filemap_setitem (hFileMap, &key, &value);
        assert (ret == 0);
    }

    /* 修改后查询 */
    for (auto it = cMapMod.cbegin(); it != cMapMod.cend(); ++it)
    {
        FILEMAP_KEY key = {};
        FILEMAP_VALUE value = {};
        snprintf (key.szKey, sizeof(key.szKey), "%s", it->first.data());

        int ret = filemap_getitem (hFileMap, &key, &value);
        assert (ret == 0);
        assert (it->second == value.byteData);
    }

    // filemap_generateinfo (hFileMap, "beforedel");

    /* 随机删除 */
    srand (time(NULL));

    std::set<int> cErasedSet;
    while (1)
    {
        int num_rand = rand () % nTestNum;

        cErasedSet.insert (num_rand);
        if ((int)cErasedSet.size() >= nTestNum / 2)
        {
            break;
        }
    }
    // for (int i = 0; i < nTestNum; ++i)
    // {
    //     cErasedSet.insert (i);
    //     if ((int)cErasedSet.size() > nTestNum / 2)
    //     {
    //         break;
    //     }
    // }

    std::list<std::string> cErasedList;
    for (auto it = cErasedSet.cbegin(); it != cErasedSet.cend(); ++it)
    { /* 随机删除一半的元素 */

        FILEMAP_KEY key = {};
        int nNum = *it;
        snprintf (key.szKey, sizeof(key.szKey), "name%d_%d", nNum, nNum * nNum);

        int ret = cMapMod.erase (key.szKey);
        assert (ret == 1);
        _debug ("delete <%s>\n", key.szKey);
        ret = filemap_deleteitem (hFileMap, & key);
        if (ret != 0)
        {
            filemap_generateinfo (hFileMap, szFileName);
            _error ("delete key=%s failed\n", key.szKey);
        }
        assert (ret == 0);

        cErasedList.push_back (key.szKey);
    }

    // filemap_generateinfo (hFileMap, "afterdelete");

    /* 删除后对比 */
    for (auto it = cErasedList.cbegin(); it != cErasedList.cend(); ++it)
    { /* 被删除元素 */
        auto ret = cMapMod.find (*it);
        assert (ret == cMapMod.cend());

        FILEMAP_KEY key = {};
        snprintf (key.szKey, sizeof(key.szKey), "%s", it->data());
        int ret_exist = filemap_existitem (hFileMap, & key);
        if (ret_exist != 0)
        {
            _error ("exist item <%s> failed, ret=%d\n", key.szKey, ret_exist);
        }
        assert (ret_exist == 0);

        FILEMAP_VALUE value = {};
        int ret_getitem = filemap_getitem (hFileMap, &key, &value);
        assert (ret_getitem < 0);
    }

    for (auto it = cMapMod.cbegin(); it != cMapMod.cend(); ++it)
    { /* 未被删除元素 */
        FILEMAP_KEY key = {};
        snprintf (key.szKey, sizeof(key.szKey), "%s", it->first.data());
        int ret_exist = filemap_existitem (hFileMap, &key);
        assert (ret_exist == 1);

        FILEMAP_VALUE value = {};
        int ret_getitem = filemap_getitem (hFileMap, &key, &value);
        assert (ret_getitem == 0);
        assert (it->second == value.byteData);

        _debug ("[%s]=%s\n", it->first.data(), it->second.data());
    }

    _debug ("total=%d\n", (int)cMapMod.size());

    filemap_generateinfo (hFileMap, szFileName);

    int ret_close = filemap_close (hFileMap);
    assert (ret_close == 0);

    return 0;
}

/**
 * 对单文件执行重复载入操作
 * 利用已有的结构 std::map 来对filemap进行检查
 * 两者进行完全一致的操作，行为也应该完全一致
 */
static int test_filemap_reload (int nTotalNum, const char *szFileName)
{
    char szObjFileName[64] = {};
    snprintf (szObjFileName, sizeof(szObjFileName), "%s_%d.dat", __FUNCTION__, nTotalNum);

    _debug ("reload test: <num=%d>\n", nTotalNum);

    srand(time(NULL));

    std::map<std::string, std::string> mapBackUp;
    for (int nTestCount = 0; nTestCount <= 10; ++nTestCount)
    { /* 每次循环执行一次对文件的完整操作 */

        const int nAddCount = rand () % (nTotalNum + 1);
        const int nGetCount = rand () % (nTotalNum + 1);
        const int nDelCount = rand () % (nTotalNum + 1);

        _debug ("testCount=%d,add=%d,get=%d,del=%d\n", nTestCount, nAddCount, nGetCount, nDelCount);

        /* 打开对象 */
        FILEMAP_HANDLE hFileMap = 0;
        if (access (szObjFileName, F_OK) == 0)
        {
            hFileMap = filemap_load (szObjFileName);
        }
        else 
        {
            hFileMap = filemap_create (szObjFileName, nTotalNum);
        }

        assert (hFileMap != 0);

        /* 写入操作 */
        for (int i = 0; i < nAddCount; ++i)
        {
            FILEMAP_KEY key = {};
            FILEMAP_VALUE value = {};

            int k = rand () % (2 * nTotalNum);

            snprintf(key.szKey, sizeof(key.szKey), "name_%d_%d_%d", k, k * k, k * k * k);
            snprintf(value.byteData, sizeof(value.byteData), "value_%d", k);

            auto it = mapBackUp.find (key.szKey);
            if (it == mapBackUp.cend())
            { /* 如果是新元素 */
                if ((int)mapBackUp.size() < nTotalNum)
                { /* 如果没有满，就加进去 */
                    mapBackUp.insert (std::pair<std::string,std::string>(key.szKey, value.byteData));
                    int ret = filemap_setitem(hFileMap, &key, &value);
                    assert(ret == 0);
                }
                else 
                { /* 如果满了，就会失败 */
                    int ret = filemap_setitem(hFileMap, &key, &value);
                    assert(ret < 0);
                }
            }
            else 
            { /* 如果是已有元素 */
                mapBackUp.insert(std::pair<std::string, std::string>(key.szKey, value.byteData));
                int ret = filemap_setitem(hFileMap, &key, &value);
                assert(ret == 0);
            }
        }

        /* 查询操作 */
        for (int i = 0; i < nGetCount; ++i)
        {
            FILEMAP_KEY key = {};
            FILEMAP_VALUE value = {};

            int k = rand () % (2 * nTotalNum);

            snprintf(key.szKey, sizeof(key.szKey), "name_%d_%d_%d", k, k * k, k * k * k);

            auto it = mapBackUp.find (key.szKey);
            int ret_getitem = filemap_getitem (hFileMap, &key, &value);
            if (it != mapBackUp.cend())
            {
                assert (ret_getitem == 0);
                assert (it->second == value.byteData);
            }
            else 
            {
                assert  (ret_getitem < 0);
            }
        }

        /* 删除操作 */
        for (int i = 0; i < nDelCount; ++i)
        {
            FILEMAP_KEY key = {};

            int k = rand() % (2 * nTotalNum);

            snprintf(key.szKey, sizeof(key.szKey), "name_%d_%d_%d", k, k * k, k * k * k);

            int nNum = (int)mapBackUp.erase (key.szKey);
            if (nNum == 0)
            {
                int ret = filemap_deleteitem (hFileMap, &key);
                assert (ret < 0);
            }
            else 
            {
                int ret = filemap_deleteitem (hFileMap, &key);
                assert (ret == 0);
            }
        }

        /* 一致性检查 */
        for (auto it = mapBackUp.cbegin(); it != mapBackUp.cend(); ++it)
        {
            FILEMAP_KEY key = {};
            FILEMAP_VALUE value = {};

            snprintf (key.szKey, sizeof(key.szKey), "%s", it->first.data());
            int ret = filemap_getitem (hFileMap, &key, &value);
            assert (ret == 0);
            assert (it->second == value.byteData);
        }

        /* 关闭对象 */
        int ret_close = filemap_close (hFileMap);
        assert (0 == ret_close);
    }

    return 0;
}

/* 增删改查的小量测试 */
int test_filemap ()
{
    test_filemap_normal (10, 1, "10_1.txt");
    test_filemap_normal (10, 2, "10_2.txt");
    test_filemap_normal (10, 5, "10_5.txt");
    test_filemap_normal (10, 10, "10_10.txt");
    test_filemap_normal (1000, 1, "1000_1.txt");
    test_filemap_normal (1000, 5, "1000_5.txt");
    test_filemap_normal (1000, 100, "1000_100.txt");
    test_filemap_normal (1000, 501, "1000_501.txt");
    test_filemap_normal (1000, 999, "1000_999.txt");
    test_filemap_normal (1000, 1000, "1000_1000.txt");

    return 0;
}


/** 
 * 单文件的重复载入操作测试 
 * 注意：测试前要清空当前文件夹。
 */
int test_filemap1 ()
{
    int nNum[] = {
        5, 9, 10, 11, 15, 18, 19, 21, 24, 57, 60, 100, 158, 950, 1342, 2399, 5104, 8907, 10000, 10001,
    };
    for (int i = 0; i < (int)(sizeof(nNum) / sizeof(nNum[0])); ++i)
    {
        char szFileName[64] = {};
        snprintf (szFileName, sizeof(szFileName), "filemap_%s_%d.txt", __FUNCTION__,nNum[i]);
        test_filemap_reload (nNum[i], szFileName);
    }

    return 0;
}