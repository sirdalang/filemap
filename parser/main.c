

#include <sys/stat.h>
#include <sys/types.h>

#include <stdio.h>
#include <stdlib.h>

#include "../filemap.h"

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

int main (int argc, const char **argv)
{
    if (argc < 3)
    {
        _error ("usage: %s <input_file, output_file>\n", argv[0]);
        return -1;
    }

    const char *szSrcFile = argv[1];
    const char *szDstFile = argv[2];

    struct stat sStat = {};
    if (stat (szSrcFile, &sStat) < 0)
    {
        _error ("stat <%s> failed\n", szSrcFile);
        return -1;
    }

    if (! S_ISREG (sStat.st_mode))
    {
        _error ("<%s> is not file\n", szSrcFile);
        return -1;
    }

    char szBackFile[64] = "./parser.bak";

    char cmd [64] = {};
    snprintf (cmd, sizeof(cmd), "cp %s %s -rf", szSrcFile, szBackFile);
    system (cmd);

    FILEMAP_HANDLE hFileMap = filemap_load (szBackFile);
    if (0 == hFileMap)
    {
        _error ("load failed\n");
        return -1;
    }

    if (filemap_generateinfo (hFileMap, szDstFile) < 0)
    {
        _error ("generate info failed\n");
        return -1;
    }

    if (filemap_close (hFileMap) < 0)
    {
        _error ("close filemap failed\n");
        return -1;
    }

    return 0;
}