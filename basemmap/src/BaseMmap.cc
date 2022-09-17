#include "BaseMmap.h"

using namespace shm;

long CBaseMmap::m_pageSize = 4096;

CBaseMmap::CBaseMmap( size_t itemsize, size_t itemcapacity,size_t extend_sz /*10*1024*1024*/,CModeType modetype /*= 1*/ ):
    m_itemsize(itemsize),m_itemcapacity(itemcapacity),m_realitemcap(0),m_initSize(0),m_totalSize(0), 
    m_extendSize( extend_sz ),m_modetype(modetype),m_pheader(nullptr){
    long pagesize = sysconf(_SC_PAGE_SIZE);
    m_pageSize = pagesize==-1?4096:pagesize;
    m_filename[0] = '\0';
    m_fd = -1;
    m_vmStartAddr = (void*)MAP_FAILED;
}

CBaseMmap::~CBaseMmap(){
    CloseFile();
}

bool CBaseMmap::MapFile(){
    int prot;
    if (m_modetype == M_READWRITE){
        prot = PROT_READ|PROT_WRITE;
    }else if ( m_modetype == M_READ ){
        prot = PROT_READ;
    }else{
        prot = PROT_READ|PROT_WRITE;
    }
    if ((m_vmStartAddr=mmap(nullptr, m_totalSize, prot,MAP_SHARED, m_fd, 0)) == MAP_FAILED)
        return false;
    m_pheader = (CMmapHeader*)m_vmStartAddr;
    return true;
}

bool CBaseMmap::InitHeader(){
    m_pheader->m_headersize = HEADER_SIZE;
    m_pheader->m_version = HEADER_VERSION;
    m_pheader->m_itemsize = m_itemsize;
    m_pheader->m_itemcount = 0;
    m_pheader->m_realcapacity = m_itemcapacity;
    m_pheader->m_pre_extend_itemcap = 0;
    m_pheader->m_nextwritepos = 0;
    return true;
}

bool CBaseMmap::CreateAndInitFile(){
    if( IsBeenMmap() ){
        return true;
    }

    if( !MakeDir(m_filename ) ){
        return false;
    }

    if ((m_fd = open(m_filename, O_CREAT|O_RDWR|O_TRUNC, 00660)) == -1) {
        if (errno == ENFILE || errno == ENOMEM)
            return false;
        else
            return false;
    }

    if ((m_totalSize=lseek(m_fd, 0, SEEK_END)) < m_initSize){
        ExtendFile(m_initSize - m_totalSize);
    }

    if( !MapFile() )
        return false;
    InitHeader();
    return true;
}

bool CBaseMmap::ExtendFile( size_t len ){
     if (m_fd < 0 && (m_fd = open(m_filename, O_CREAT | O_RDWR, 00660)) == -1) {
         if (errno == ENFILE || errno == ENOMEM)
             return false;
         else
             return false;
     }
     if (ftruncate(m_fd, m_totalSize+len) == 0) {
        m_totalSize += len;
        char buf[1] = "";
        if (pwrite(m_fd, buf, 1, m_totalSize) < 0) {
            return false;
        }
    } else {
        return false;
    }
    return true;
}

bool CBaseMmap::SaveData( char *syn_buf_start, size_t syn_buf_len, int sync_flag /*= MS_ASYNC*/ ){
    if (m_modetype == M_READ)
        return false;
    void* syncAddr = nullptr;
    size_t syncLen = 0;
    if (syn_buf_len == 0) {
        syncAddr = m_vmStartAddr;
        syncLen = m_totalSize;
    } else {
        syncAddr = syn_buf_start;
        syncLen = syn_buf_len;
    }
    if (msync(syncAddr, syncLen, sync_flag) == -1){
        return false;
    }
    return true;
}

bool CBaseMmap::Myclose(){
    if( m_fd > 0 ){
        close( m_fd );
        m_fd = -1;
    }
    return true;
}

bool CBaseMmap::CreateAndMapFile( const string& filename ){
    if( filename.size() == 0 ){
        return false;
    }
    int filenamelen = filename.length();
    memcpy(m_filename, filename.c_str(), filenamelen);
    m_filename[filenamelen] = '\0';
    
    /* 此处调用create如果已经存在映射需要释放，之后才能正确的重建 */
    CloseFile();
    bool ret = false;
    if (!(ret = CreateAndInitFile())) {
        CloseFile();
        return ret;
    }
    Myclose();
    return ret;
}

bool CBaseMmap::OpenAndMapFile(const string& filename ){
    bool ret;
    if( IsBeenMmap() ){
        return true;
    }

    int filenamelen = filename.length();
    memcpy(m_filename, filename.c_str(), filenamelen);
    m_filename[filenamelen] = '\0';
    if ((m_fd = open(m_filename, O_RDWR | O_APPEND)) == -1) {
        return false;
    }
    off_t fileSize = lseek(m_fd, 0, SEEK_END);
    if( fileSize == (off_t)-1 ){
        return false;
    }
    m_totalSize = fileSize;
    m_initSize = fileSize;
    m_extendSize = m_initSize;
    ret = MapFile();
    if( ret ){
        m_itemsize = m_pheader->m_itemsize;
        m_itemcapacity = m_pheader->m_realcapacity;
        m_realitemcap = m_pheader->m_realcapacity;
    }
    Myclose();
    return ret;
}

bool CBaseMmap::SampleMapFile( const string& filename ){
    if (IsExistFile(filename)) {
        return OpenAndMapFile(filename);
    }
    //读模式不能创建文件
    if( m_modetype == M_READ )
        return false;
    if( m_initSize == 0 ){
        int redidue = m_pageSize - (m_itemsize * m_itemcapacity + HEADER_SIZE)%m_pageSize;
        m_initSize = m_itemsize * m_itemcapacity + HEADER_SIZE + redidue;
        m_realitemcap = m_itemcapacity + redidue/m_itemsize;
        m_extendSize = m_initSize;
    }
    return CreateAndMapFile(filename);
}

void CBaseMmap::CloseFile(){
    SaveAllModifyData();
    if (IsBeenMmap()) {
        munmap(m_vmStartAddr, m_totalSize);
        m_vmStartAddr = (void*)MAP_FAILED;
    }
    Myclose();
}

//顺序写入数据
//每次写入之后都需要判断是否修改nextwriteoffset
int CBaseMmap::WriteData( void* data, size_t count, bool sysncflag /*= false*/ ){
    if (!data || count == 0 || m_modetype == M_READ) {
        return -1;
    }
    size_t offset = m_pheader->m_nextwritepos*m_pheader->m_itemsize+m_pheader->m_headersize;
    memcpy((char*)m_vmStartAddr+ offset, data, count);
    if( sysncflag ){
        char *syncaddr = (char*)m_vmStartAddr + 
            (offset&~(m_pageSize-1));
        SaveData(syncaddr, count, MS_SYNC);
    }
    return m_pheader->m_nextwritepos;
}

//指定位置写入偏移量
//外部需要重新输入nextWritePos的位置
bool CBaseMmap::WriteData( size_t offset,void* data, size_t count, bool sysncflag /*= false*/ ){
    if (!data || count == 0 || offset < HEADER_SIZE || m_modetype == M_READ) {
        return false;
    }
    memcpy((char*)m_vmStartAddr+offset, data, count);
    if( sysncflag ){
        char *syncaddr = (char*)m_vmStartAddr + 
            (offset&~(m_pageSize-1));
        SaveData(syncaddr, count, MS_SYNC);
    }
    return true;
}

bool CBaseMmap::SaveAllModifyData(){
    if (m_vmStartAddr == MAP_FAILED) {
        return false;
    }

    if( m_modetype == M_READ )
        return false;

    if (msync(m_vmStartAddr, m_totalSize, MS_ASYNC) == 0) {
        fflush(NULL);
        return true;
    }
    return false;
}

//buf需要外部定义好之后传入
//该dataoffset包含mmap头的长度了已经
bool CBaseMmap::ReadData( size_t data_offset, void* buf, size_t count ){
    if ( buf == nullptr || count == 0 ){
        return true;
    }
    memcpy(buf, (char*)m_vmStartAddr+data_offset, count);
    return true;
}

//扩张mmap的文件
//默认的扩展方式是空间翻一倍
bool CBaseMmap::ExtendFileAndMap(size_t count){
    if (m_vmStartAddr == (char*)MAP_FAILED) {
        return false;
    }
    SaveAllModifyData();
    if (munmap(m_vmStartAddr, m_totalSize) < 0){
        return false;
    }
    m_vmStartAddr = (void*)MAP_FAILED;
    if( !ExtendFile(m_extendSize)){
        Myclose();
        return false;
    }
    MapFile();
    m_pheader->m_pre_extend_itemcap = m_pheader->m_realcapacity;
    m_pheader->m_realcapacity  += m_extendSize/m_itemsize ;
    Myclose();
    return true;
}
