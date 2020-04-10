
#include "hash.h"


int BKDRHash(const char *str)
{
    register int hash = 0;
    while (1)
    {
        int ch = (int)*str++;
        if (!ch)
        {
            break;
        }
        hash = hash * 131 + ch;
    }
    return hash;
}