/*
 *    功能说明: 此模块是针对mmap的基本操作的封装
 *    目前只考虑linux平台
 */

#ifndef _H_BASE_MMAP_H__
#define _H_BASE_MMAP_H__

#include <stddef.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/mman.h>
#include <sys/types.h>
#include "Tools.h"

using std::string;

namespace shm{

enum CModeType{
    M_READWRITE = 1,
    M_READ = 2
};

/*
 * m_headersize -- 文件头的大小
 * m_pre_extend_itemcap---扩容前的大小，默认是0，表示没有扩展
 */
struct CMmapHeader{
    size_t m_headersize;
    size_t m_version;
    size_t m_itemsize;
    size_t m_itemcount;
    size_t m_realcapacity;
    size_t m_pre_extend_itemcap; 
    size_t m_nextwritepos;
    CMmapHeader():m_headersize(0),m_version(0),m_itemsize(0),
    m_itemcount(0),m_realcapacity(0),m_pre_extend_itemcap(0),
    m_nextwritepos(0){
    }
};

const int HEADER_SIZE = sizeof( CMmapHeader );
const int HEADER_VERSION = 100;
const int EXTEND_SIZE = 10*1024*1024;

class CBaseMmap{
public:
    CBaseMmap( size_t itemsize, size_t itemcapacity, size_t extend_sz = 10*1024*1024,CModeType modetype=M_READWRITE );
    ~CBaseMmap();

private:
    static long m_pageSize; 
private:
    char m_filename[PATH_MAX];
    size_t m_itemsize;
    size_t m_itemcapacity; //传参的传入的个数  目前head保存的是该值
    size_t m_realitemcap; //mmap真是的capacity
    size_t m_initSize;
    size_t m_totalSize;
    size_t m_extendSize;
    int m_fd;
    CModeType m_modetype; //1: 读写(默认)， 2: 只读

    void* m_vmStartAddr;
    CMmapHeader* m_pheader;
private:
    bool InitHeader();
    bool CreateAndInitFile();
    bool ExtendFile( size_t len );
    bool MapFile();
    bool SaveData( char *syn_buf_start, size_t syn_buf_len, int sync_flag = MS_ASYNC );
    bool Myclose();
public:
    bool CreateAndMapFile( const string& filename );
    bool OpenAndMapFile(const string& filename );
    bool SampleMapFile( const string& filename );
    void CloseFile();
    int WriteData( void* data, size_t count, bool sysncflag = false );
    bool WriteData( size_t offset,void* data, size_t count, bool sysncflag = false );
    bool SaveAllModifyData();
    //从偏移量data_offset,读取count个自己到buf中
    //buf由使用方来进行分配和释放
    bool ReadData( size_t data_offset, void* buf, size_t count );
    bool ExtendFileAndMap(size_t count = 0);

    //文件是否已经被mmap
    bool IsBeenMmap() const {
        return m_vmStartAddr==MAP_FAILED?false:true;
    }

    void SetExtendSize( const size_t extendsize ){ m_extendSize = extendsize; }
    void SetInitSize( const size_t initsize ){ m_initSize = initsize; }
    //重新设置下次需要写入的位置
    inline void SetNextWritepos( const size_t pos);
    inline void SetItemCount( const size_t itemcount );
    inline void* GetvmAddr();
    inline void* GetDataStartAddr();
    inline CMmapHeader* GetHeaderaddr();
    
    //获取mmap中存储数据的大小
    inline size_t GetDataSize();
    inline size_t GetCapacity();
};

inline void CBaseMmap::SetNextWritepos( const size_t pos){
    m_pheader->m_nextwritepos = pos;
}

inline void CBaseMmap::SetItemCount( const size_t itemcount ){
    m_pheader->m_itemcount = itemcount;
}

inline void* CBaseMmap::GetvmAddr(){
    return m_vmStartAddr;
}

inline void* CBaseMmap::GetDataStartAddr(){
    return (char*)m_vmStartAddr + HEADER_SIZE;
}

inline CMmapHeader* CBaseMmap::GetHeaderaddr(){
    return m_pheader;
}

inline size_t CBaseMmap::GetDataSize(){
    return m_totalSize - HEADER_SIZE;
}

inline size_t CBaseMmap::GetCapacity(){
    return m_pheader->m_realcapacity;
}

}
#endif
