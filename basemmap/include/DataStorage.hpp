/*
 *   针对basemmap的一层抽象封装
 *   内部包含两个mmap对象，一个是保存具体的数据, 一个是保存数据的bit位，
 *   bit位的数据会尽量分配,目前第一次分配是传入数量的8倍,
 *
 *   第一版不支持并发写
 *   m_ratio  >=1.0-- 表示不扩容
 */

#ifndef _H_DATA_STORAGE_H__
#define _H_DATA_STORAGE_H__

#include <string>
#include <vector>
#include <stdlib.h>
#include <string.h>
#include <atomic>
#include <iostream>
#include "BaseMmap.h"

using std::string;
using std::vector;
using std::atomic;

namespace shm{

#define SEQ_INSERT_FLAG (-1)

enum STO_RESULT{
    STO_OK = 0,
    STO_FAIL = -1, //处理失败
    STO_EXIST = -2, // 冲突--传入的位置已经存在数据
    STO_ILLEGAL_POS = -3, // pos非法
    STO_NORESULT = -4, //表示查找pos无数据
    STO_NOWRITE = -5 //不可写
};

//针对插入的冲突外部的调用来解决
template<typename T>
class CDataStorage {
public:
    /*
     *    datafilename--数据文件名
     *    bitfilename  -- bit位文件名
     *    itemcapacity  --初始化容量大小
     *    modetype  --初始化读写类型，默认是读写的方式
     *    ratio  --扩容的条件， 入宫>=1.0,表示不做扩容
     */
    CDataStorage( string& datafilename, string& bitfilename, size_t itemcapacity, 
            CModeType modetype = M_READWRITE, double ratio = 0.8 );
    ~CDataStorage();
    /*
     * 相关参数初始化
     */
    bool Init();

    /*
     *   插入到指定的位置，
     *   pos == SIZE_MAX: 表示不按照指定的位置插入，插入到消息头的 m_nextwriteoffset的位置
     *   imput:  data: 插入的数据  
     *       pos : 数据插入的下标
     *       如果知道的pos有数据，不做处理直接返回
     *   output: realstoragepos -- 数据真实插入的下标 --顺序插入的时候需要返回的参数
     */

    STO_RESULT InsertData( T& data, size_t& realstoragepos,size_t pos = SIZE_MAX);

    //muldata 连续插入，位置不需要连续,返回数据插入的位置
    //void InsertData( vector<T>& muldata );
    /*
     *  此函数只针对读模式的情况下
     */
    const T* FindDataPtr( size_t pos);

    /*
     *    参数说明： 
     *    pos --查找的数据所在的下标
     *    buf --返回的结果存储
     *    readcount  --读取的字节数,必须是itemsize的整数倍
     *    pos---位置数据不存在则返回无结果
     */
    STO_RESULT FindData( size_t pos, void* buf, size_t readcount);
    
    /*
     *    删除指定位置的数据 --假删除，只是把bit位置成0
     *    input: pos 删除数据所在的下标
     */
    STO_RESULT DeleteData( size_t pos);

    /*
     *    更新指定pos中的数据(直接覆盖),
     *    STO_NORESULT --表示pos位置不存在数据,不做任何的处理
     *    input: data: 更新的数据  pos:更新的下标
     */
    STO_RESULT UpdateData( T& data, size_t pos);

    /*
     *    此函数同UpdateData函数的区别就是，如果pos位置不存在数据，则执行插入操作
     *    input: data: 数据， pos:指定的下标
     */
    STO_RESULT InsertAndUpdateData( T& data, size_t pos);

    /*
     *    判断pos的位置是否存在数据
     */
    inline bool ChangeExist( size_t pos);

    /*
     *    获取mmap的容量
     */
    inline size_t GetItemCapacity();

    /*
     *   获取目前已经在mmap中的数据量
     */
    inline size_t GetStorageItemCount();

    /*
     *  获取下次能够写入的数据位置
     */
    inline size_t GetNextWritePos();
    /*
     *  数据同步到disk
     */
    STO_RESULT SaveToDisk();

    /*
     *  扩展mmap的大小(bitmmap/datammap)
     */
    bool ExtendSize();
private:
    void Set( size_t pos );
    bool Get( size_t pos );
    void Del( size_t pos );
    /*
     *  往右侧查找空闲位置,如果查找失败，则从左侧查找一个空闲位置
     *  一定会查找到一个空闲位置,此处返回的结果不做0值判断
     *    ( ratio = 0.8 ) 会扩展mmap的大大小，不会存在写满的情况
     */
    size_t GetIdlepos( size_t startpos );
    inline size_t Getoffset( size_t pos );
    void WriteHeaderInfo();
private:
    size_t m_itemcapacity;
    size_t m_itemsize;
    size_t  m_deletepos;//维护删除的位置，保留最新删除的位置
    CModeType m_modetype;
    double m_ratio;
    atomic<size_t> m_storageItemcount;
    size_t m_nextwritepos;
    string m_datafilename; 
    string m_bitfilename;
    char* m_bitdataAddr;
    void* m_bitAddr;
    void* m_dataAddr;
    CBaseMmap m_datammap;
    CBaseMmap m_bitmmap;
};

/*
 *  默认第一次加载，bitmap中能够处理的个数是datammap中个数的8倍，减少后续因扩容而需要在分配
 */
template<typename T>
CDataStorage<T>::CDataStorage(string& datafilename,string& bitfilename,size_t itemcapacity,
         CModeType modetype,double ratio ):m_itemcapacity(itemcapacity),m_itemsize(sizeof(T)),m_deletepos(SIZE_MAX),
    m_modetype(modetype),m_ratio(ratio),m_storageItemcount(0),m_nextwritepos(0),m_datafilename(datafilename), 
    m_bitfilename(bitfilename),m_bitdataAddr(nullptr),m_bitAddr(nullptr),m_dataAddr(nullptr),
    m_datammap(sizeof(T),itemcapacity,EXTEND_SIZE,modetype), m_bitmmap(1, itemcapacity,EXTEND_SIZE, modetype){
}

template<typename T>
CDataStorage<T>::~CDataStorage(){
    SaveToDisk();   
    m_bitmmap.CloseFile();
    m_datammap.CloseFile();
}

template<typename T>
void CDataStorage<T>::Set( size_t pos ){
    int bkt = pos/8;
    int offset = pos%8;
    char* ptr = m_bitdataAddr + bkt;
    *ptr |= ( 1 << offset );
}

template<typename T>
bool CDataStorage<T>::Get( size_t pos ){
    int bkt = pos/8;
    int offset = pos%8;
    char* ptr = m_bitdataAddr + bkt;
    return (*ptr & (1 << offset)) == 0 ? false : true;
}

template<typename T>
void CDataStorage<T>::Del( size_t pos ){
    int bkt = pos/8;
    int offset = pos%8;
    char* ptr = m_bitdataAddr + bkt;
    *ptr ^= (1 << offset);
}

template<typename T>
size_t CDataStorage<T>::GetIdlepos( size_t startpos ){
    if (m_deletepos != SIZE_MAX)
        return m_deletepos;
    size_t i = startpos++;
    for( ; i<m_itemcapacity; i++  ){
        if( Get(i) )
            continue;
        break;
    }

    if( i >= m_itemcapacity ){
        for( i = startpos--; i >=0; i--){
            if( Get(i) )
                continue;
            break;
        }
    }
    //return i == 0 ? -1: i;
    return i;
}

template<typename T>
inline size_t CDataStorage<T>::Getoffset( size_t pos ){
    return pos * m_itemsize + HEADER_SIZE;
}

template<typename T>
void CDataStorage<T>::WriteHeaderInfo(){
    m_bitmmap.SetItemCount( m_storageItemcount.load() );
    m_datammap.SetItemCount( m_storageItemcount.load() );
    m_bitmmap.SetNextWritepos( m_nextwritepos );
    m_datammap.SetNextWritepos(  m_nextwritepos  );
}

template<typename T>
bool CDataStorage<T>::Init(){
    if ( !m_bitmmap.SampleMapFile( m_bitfilename ) ){
        return false;
    }
    m_bitdataAddr=(char*)m_bitmmap.GetDataStartAddr(); 
    if( !m_datammap.SampleMapFile( m_datafilename ) ){
        return false;
    }
    m_bitAddr = m_bitmmap.GetvmAddr();
    m_dataAddr = m_datammap.GetvmAddr();
    //此处赋值的缘由: 当打开的文件存在时，以文件头中存储的数据为准
    //如果是第一次创建，则返回值是和传入值一样的
    m_nextwritepos = m_bitmmap.GetHeaderaddr()->m_nextwritepos;
    m_storageItemcount.store(m_bitmmap.GetHeaderaddr()->m_itemcount);
    m_itemcapacity = m_datammap.GetHeaderaddr()->m_realcapacity;
    m_itemsize =m_datammap.GetHeaderaddr()->m_itemsize;
    return true;
}

/*
 *    pos > 0 && pos > m_itemcapacity: 此处首先判断p>0,是为了防止内存溢出
 */
template<typename T>
STO_RESULT CDataStorage<T>::InsertData( T& data, size_t& realstoragepos, size_t  pos){
    if( (pos != SIZE_MAX && pos > m_itemcapacity) 
            || m_storageItemcount.load() > m_itemcapacity ) {//不合法的位置
        return STO_ILLEGAL_POS;
    }
    if ( m_modetype == M_READ )
        return STO_NOWRITE;
    STO_RESULT ret = STO_OK;
    //插入到内部的指定位置
    if( pos == SIZE_MAX ){
        int result = m_datammap.WriteData( &data, m_itemsize);
        if( result < 0 )
            return STO_FAIL;
        {
            realstoragepos = m_nextwritepos;
            Set( m_nextwritepos );
            m_storageItemcount++;
            if(m_nextwritepos==m_deletepos) {
                m_deletepos = SIZE_MAX;
            }
            m_nextwritepos = GetIdlepos( m_nextwritepos );
            WriteHeaderInfo();
        }
    } else {
        bool flag = Get(pos);
        if( flag ){
            //pos存在数据
            return STO_EXIST;
        }else{
            //写入到入参的指定位置
            if( m_datammap.WriteData( Getoffset( pos ), &data, m_itemsize ) ){
                m_storageItemcount++;
                Set( pos );
                realstoragepos = pos;
                if ( m_nextwritepos == pos ){
                    if(m_nextwritepos==m_deletepos) {
                        m_deletepos = SIZE_MAX;
                    }
                    m_nextwritepos = GetIdlepos( m_nextwritepos );
                    WriteHeaderInfo();
                }
            }else{
               //写入失败
               return STO_FAIL;
            }
        }
    }

    if( m_storageItemcount.load()/(m_itemcapacity*1.0)  >= m_ratio ){
        //需要扩容
        ExtendSize();
    }
    return ret;
}

/*
 *  本函数只有在只读的模式下才能调用
 */
template<typename T>
const T* CDataStorage<T>::FindDataPtr( size_t pos){
    if( pos < 0 || pos > m_itemcapacity )   
        return NULL;

    if( Get( pos )){
       return (T*)((char*)m_dataAddr + Getoffset(pos));
    }
    return NULL;
}

template<typename T>
STO_RESULT CDataStorage<T>::FindData( size_t pos, void* buf, size_t readcount){
    if( pos < 0 || pos > m_itemcapacity)   
        return STO_ILLEGAL_POS;
    if( Get(pos) ){
        if( m_datammap.ReadData( Getoffset(pos),buf,readcount ) )
            return STO_OK;
        return STO_FAIL;
    }
    return STO_NORESULT;
}
    
template<typename T>
STO_RESULT CDataStorage<T>::DeleteData( size_t pos){
    if( pos < 0 || pos > m_itemcapacity)   
        return STO_ILLEGAL_POS;
    if ( m_modetype == M_READ )
        return STO_NOWRITE;
    if( Get(pos) ){
        Del( pos );
        m_storageItemcount --;
        m_deletepos = pos;
    }
    return STO_OK;
}

template<typename T>
STO_RESULT CDataStorage<T>::UpdateData( T& data, size_t pos){
    if( pos < 0 || pos > m_itemcapacity )   
        return STO_ILLEGAL_POS;
    if ( m_modetype == M_READ )
        return STO_NOWRITE;
    if( Get(pos) ){ //write data
         m_datammap.WriteData( Getoffset( pos ), &data, m_itemsize );       
         return STO_OK;
    }
    return STO_NORESULT;
}

template<typename T>
STO_RESULT CDataStorage<T>::InsertAndUpdateData( T& data, size_t pos){
    if( pos < 0 || pos > m_itemcapacity )   
        return STO_ILLEGAL_POS;
    if ( m_modetype == M_READ )
        return STO_NOWRITE;
    if( Get(pos) ){
         m_datammap.WriteData( Getoffset( pos ), &data, m_itemsize );       
         return STO_OK;
    }else{
        size_t realstoragepos;
        return InsertData( data, realstoragepos, pos );
    }
}

template<typename T>
inline bool CDataStorage<T>::ChangeExist( size_t pos){
    return Get(pos);
}

template<typename T>
inline size_t CDataStorage<T>::GetItemCapacity(){
    return m_itemcapacity;
}

template<typename T>
inline size_t CDataStorage<T>::GetStorageItemCount(){
    return m_storageItemcount.load();
}

template<typename T>
inline size_t CDataStorage<T>::GetNextWritePos(){
    return m_nextwritepos;
}

template<typename T>
STO_RESULT CDataStorage<T>::SaveToDisk(){
    if ( m_modetype == M_READ )
        return STO_NOWRITE;
    WriteHeaderInfo();
    m_bitmmap.SaveAllModifyData();
    m_datammap.SaveAllModifyData();
    return STO_OK;
}

template<typename T>
bool CDataStorage<T>::ExtendSize(){
    //首先扩展数据mmap的大小
    bool flag = m_datammap.ExtendFileAndMap();
    if( flag ){
        //根据扩展之后的itemcap来判断bitmap是否需要扩展
        size_t extendItemnm = m_datammap.GetCapacity();
        size_t bitmmapcount = m_bitmmap.GetDataSize();
        m_dataAddr = m_datammap.GetvmAddr();
        m_itemcapacity = extendItemnm;
        if( m_itemcapacity > bitmmapcount*8 ){
            flag = m_bitmmap.ExtendFileAndMap();
            if( flag ){
                m_bitAddr = m_bitmmap.GetvmAddr();
                m_bitdataAddr=(char*)m_bitmmap.GetDataStartAddr(); 
            }else{
                printf( "extend bit mmap fail\n" );
            }
        }else{
            //触发一下bitmmap的数据同步
            m_bitmmap.SaveAllModifyData();
        }
    }
    return flag;
}
}
#endif
