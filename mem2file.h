

/**
 * filename: mem2file.h
 * date: 20200331
 * os: linux
 * 
 * description:
 * 内存与文件映射
 * 将流式文件操作更改为块式内存操作
 * 
 */


#ifdef __cplusplus
extern "C" {
#endif 

typedef void * MEM2FILE_HANDLE;

/**
 * @brief mem2file_create 创建实例
 * @param [IN] szFileName 绑定的文件
 * @return 失败返回0，否则返回新创建的实例句柄
 */
MEM2FILE_HANDLE mem2file_create(const char *szFileName);

/**
 * @brief mem2file_close 关闭实例，并释放对应的资源
 * @param [IN] hInstance 实例句柄
 * @return 成功返回0，否则返回-1
 * @note 不能多次关闭一个实例
 */
int mem2file_close(MEM2FILE_HANDLE hInstance);

/**
 * @brief mem2file_size 获取实例的大小
 * @param [IN] hInstace 实例句柄
 * @param [OUT] pSize 实例的大小
 * @return 成功返回0，否则返回-1
 */
int mem2file_size (MEM2FILE_HANDLE hInstance, int *pnSize);

/**
 * @brief mem2file_resize 修改实例的大小
 * @param [IN] hInstace 实例句柄
 * @param [IN] nSize 新的实例大小
 * @return 成功返回0，否则返回-1
 * @note 扩展的区域数据被填充为0。当由小扩大时，耗时。
 */
int mem2file_resize (MEM2FILE_HANDLE hInstance, int nSize);

/**
 * @brief mem2file_setdata 写入数据
 * @param [IN] hInstance 实例句柄
 * @param [IN] pData 数据指针
 * @param [IN] nSize 数据大小
 */
int mem2file_setdata (MEM2FILE_HANDLE hInstance, int pos, const void *pData, int nSize);

/**
 * @brief mem2file_getdata 获取数据
 * @param [IN] hInstance 实例句柄
 * @param [OUT] pData 数据指针
 * @param [IN] nSize 数据大小
 * @return 成功返回0，否则返回-1
 */
int mem2file_getdata (MEM2FILE_HANDLE hInstance, int pos, void *pData, int nSize);

/**
 * @brief mem2file_sync 写磁盘
 */
 int mem2file_sync (MEM2FILE_HANDLE hInstance);

#ifdef __cplusplus
}
#endif 

/**
 * 耗时的增长 100 200
 * 工具测
 * 读的字节数不同
 */