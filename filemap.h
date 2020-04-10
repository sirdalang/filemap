
/**
 * 一个建立在文件上的映射表
 * @note 映射表的大小是确定的，不能动态扩展
 */

#ifndef FILEMAP_H__
#define FILEMAP_H__

#ifdef __cplusplus
extern "C" {
#endif 

typedef void *FILEMAP_HANDLE;

typedef struct 
{
    char szKey[64];
} FILEMAP_DATA_64B;

typedef struct 
{
    char byteData[10 * 1024];
} FILEMAP_DATA_10K;

typedef FILEMAP_DATA_64B FILEMAP_KEY;
typedef FILEMAP_DATA_10K FILEMAP_VALUE;

/**
 * @brief filemap_create 创建实例
 * @param [IN] szFileName 绑定的文件
 * @param [IN] nNum 创建的数量
 * @return 失败返回NULL，否则返回新创建的实例句柄
 * @note 对同一个文件只应创建一个实例
 */
FILEMAP_HANDLE filemap_create (const char *szFileName, int nNum);

/**
 * @brief filemap_load 创建实例
 * @param [IN] szFileName 绑定的文件
 * @return 失败返回NULL，否则返回新创建的实例句柄
 * @note 对同一个文件只应创建一个实例
 */
FILEMAP_HANDLE filemap_load (const char *szFileName);

/**
 * @brief filemap_close 关闭实例
 * @param [IN] hInstance 实例句柄
 * @return 成功返回0，否则返回-1
 */
int filemap_close (FILEMAP_HANDLE hInstance);

/**
 * @brief filemap_existitem 检查项是否存在
 * @param [IN] key
 * @return 存在为1，否则为0
 */
int filemap_existitem (FILEMAP_HANDLE hInstance, const FILEMAP_KEY *key);

/**
 * @brief filelmap_getitem 获取一个项
 * @param [IN] key 键
 * @param [OUT] value 值
 * @return 成功返回0，否则返回-1
 * @note 应先检查是否存在
 */
int filemap_getitem (FILEMAP_HANDLE hInstance, const FILEMAP_KEY *key, FILEMAP_VALUE *value);

/**
 * @brief filemap_additem 记录一个项，存在则修改，不存在则新增
 * @param [IN] key 键
 * @param [IN] value 值
 * @return 成功返回0，否则返回-1
 * @note 如果表已满，则无法添加
 */
int filemap_setitem (FILEMAP_HANDLE hInstance, const FILEMAP_KEY *key, const FILEMAP_VALUE *value);

/**
 * @brief filemap_deleteitem 删除一个项
 * @param [IN] key 键
 * @return 成功返回0，否则返回-1
 */
int filemap_deleteitem (FILEMAP_HANDLE hInstance, const FILEMAP_KEY *key);

/**
 * @brief 生成@hInstance的信息，并输出到@szFilename中
 * @note 仅用于调试用途
 */
int filemap_generateinfo (FILEMAP_HANDLE hInstance, const char *szFileName);

#ifdef __cplusplus
}
#endif 

#endif // FILEMAP_H__