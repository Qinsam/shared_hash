#ifndef SHARED_HASH_MAP_H
#define SHARED_HASH_MAP_H

#include <iostream>
#include <set>
#include "type.h"
#include "./basemmap/include/DataStorage.hpp"
#include "shared_hash_fun.h"

/*基于mmap的hash_map实现，key只支持string,其他类型通过转换为唯一string来存储
 *k-v 支持  k对1(default)  1对k(k通过构造函数来控制)
 *通过HASH_MAP_CONF(entry_name,max_query_len,topk)宏来生成一个配置，
 *第一个参数是hash_entry的name，可以自定义随便起
 *第二个参数是max_query_le,代表支持的query的最大长度，考虑到性能和空间，qkey不能任意长，这个参数只能填数字，如:50
 *第三个参数是一个key,最多对应多少value,考虑到性能和空间，一个key暂时不支持对应任意多少value
 *
 *使用示例:
 *
 *struct A{
 *   uint32_t a;
 *   char name[50];
 *};
 *
 *A a;
 *a.a=1;
 *char buf[50]={0};
 *sprintf(buf,"%s",xxxxx);
 *strcpy(a.name,buf);
 *
 *HASH_CONF(test,50,10);
 *
 *string path="/home/test/mmap";
 *size_t bucket_num=10000000;
 *SharedHashMap<test,A> *shm=new SharedHashMap<test,A>(path,bucket_num);  //bucket_num可以省略，默认是10000000,
 *shm->Init();
 *
 *
 *
 *size_t offset_a=shm->insertObj(a);  //插入数据
 *string key="test";
 *uint8_t score=11;                   //score 用户根据业务自己计算key和value关联的分数
 *shm->map(key,offset_a,score);       //score用于选出topk，查询时按照分数排序
 *
 *查询方法：
 *DocResult<A> docs=shm->get("test");
 *docs就是最终查询结果
 *
 *
 *实现原理:
 *
 *
 *bucket   OOOOOOOOOOO     ##bucket 个数从创建后不发生改变,开始时尽量开大一点，可以减少hash冲突，
 *             \           ## 加快访问速度,后续可能加入rehash功能
 *              \
 *               \
 *hash_entry OOOOOOOOOOOOOOOOOOOOOOOOOOOOOO   ##value in bucket，自动扩容
 *               /\
 *doc    OOOOOOOOOOOO   ## 原始数据
 *
 */

namespace shm{

struct HashValueItem {
    size_t offset; //指向原始mmap数据的offset
    uint8_t score; //打分
    HashValueItem() {
        score=0;
        offset=0;
    }
};

#define HASH_MAP_CONF(entry_name,max_query_len,topk) \
struct entry_name {                   \
    struct HashValueItem item[topk];  \
    size_t next;                      \
    char term[max_query_len];         \
    size_t item_num;                  \
    entry_name() {                    \
        next=SIZE_MAX;                \
        item_num=0;                   \
    }                                 \
}

template<typename V>
struct DocValue {
    const V* doc;
    uint8_t score;
    DocValue(){
        doc=NULL;
        score=0;
    }
};

template<typename V>
struct DocResult{
    std::vector<DocValue<V> > docs;
};

template<typename ENTRY,typename V>
class SharedHashMap {
private:
    CDataStorage<V> *docData_;  //底层mmap原始数据(占用空间较小的大部分数据)
    CDataStorage<HashBucket> *hashBucket_; //hash数据入口
    CDataStorage<ENTRY> *hashValue_;  //hash数据
    ENTRY en;
    size_t bucketSize_;

public:
    SharedHashMap(string &datapath,CModeType m=M_READWRITE,
                      size_t bucket_num=10000000) {
        string bkdatafile=datapath+"/bucket.data";
        string bkbitfile = datapath+"/bucket.bit";
        string hmdatafile=datapath+"/value.data";
        string hmbitfile=datapath+"/value.bit";
        string sdocfile=datapath+"/doc.data";
        string sbitfile=datapath+"/doc.bit";
        docData_ = new CDataStorage<V>(sdocfile,sbitfile,bucket_num,m);
        hashBucket_ = new CDataStorage<HashBucket>(bkdatafile,bkbitfile,bucket_num,m,2);
        hashValue_ = new CDataStorage<ENTRY>(hmdatafile,hmbitfile,bucket_num*3,m);
    }

    ~SharedHashMap() {
        if(NULL!=docData_) {
            delete docData_;
            docData_ = NULL;
        }
        if(NULL!=hashBucket_) {
            delete hashBucket_;
            hashBucket_ = NULL;
        }
        if(NULL!=hashValue_) {
            delete hashValue_;
            hashValue_=NULL;
        }
    }

    bool Init() {
        if(!docData_->Init()) {
            std::cout<<"init doc data failed!"<<std::endl;
            return false;
        }
        if(!hashBucket_->Init()) {
            std::cout<<"init hash bucket failed!"<<std::endl;
            return false;
        }
        if(!hashValue_->Init()) {
            std::cout<<"init hash value failed!"<<std::endl;
            return false;
        }
        bucketSize_=hashBucket_->GetItemCapacity();
        return true;
    }

    bool empty() const {
        return 0==hashBucket_->GetStorageItemCount();
    }

    inline size_t hashSize() const {
        return hashValue_->GetStorageItemCount();
    }

//inline size_t bucketSize() const {
//        return hashBucket_->GetItemCapacity();
//    }

    inline size_t docSize() const {
        return docData_->GetItemCapacity();
    }

    inline const ENTRY* getValue(const string &k,size_t &entry_offset) {
        size_t offset = getBucket(k);
        const HashBucket* b=hashBucket_->FindDataPtr(offset);
        //std::cout<<"bucket "<<offset<<std::endl;
        if(NULL==b) {
            //std::cout<<"bucket "<<offset<<" is null"<<std::endl;
            return NULL;
        }
        //std::cout<<"get value found bucket "<<offset<<"header="<<b->header<<std::endl;
        const ENTRY* v=hashValue_->FindDataPtr(b->header);
        entry_offset=b->header;
        while(v!=NULL) {
            if(v->term == k) {
                //std::cout<<" get value found value offset "<<entry_offset<<std::endl;
                break;
            }
            if(v->next!=SIZE_MAX) {
                entry_offset=v->next;
                //std::cout<<"value offset "<<entry_offset<<std::endl;
                v=hashValue_->FindDataPtr(v->next);
            }else {
                v=NULL;
            }
        }
        return v;
    }

    inline const ENTRY* getValueEntry(const string &k,size_t &entry_offset) {
      size_t offset = getBucket(k);
        const HashBucket* b=hashBucket_->FindDataPtr(offset);
        if(NULL==b) {
            return NULL;
        }
        const ENTRY* v=hashValue_->FindDataPtr(b->header);
        entry_offset=b->header;
        return v;
    }

    inline size_t getBucket(const string &k) {
        size_t hashCode = hash_code(k);
        return hashCode % bucketSize_;
    }

    inline size_t insertObj(V& v) const{
        size_t pos;
        if(STO_OK!=docData_->InsertData(v,pos)) {
            //std::cout<<"insert data failed!"<<std::endl;
            return SIZE_MAX;
        }
        return pos;
    }

    /*
     *同一份数据insertObj后，可以多次调用insert,把可以和这份数据建立映射
      */
    inline int map(const string &k,size_t& obj_offset,uint8_t score=0) {
        if(k.size() > sizeof(en.term)) {
            return -1;
        }
        size_t offset = getBucket(k);
        size_t entry_offset;
        const ENTRY* obj=getValue(k,entry_offset);
        size_t tmp;
        if(NULL==obj) {
            ENTRY entry;
            strcpy(entry.term,k.c_str());
            entry.item[0].offset = obj_offset;
            entry.item[0].score=score;
            entry.item_num=1;
            if(STO_OK!=hashValue_->InsertData(entry,tmp)) {
                //std::cout<<"insert  entry failed!"<<std::endl;
                return -1;
            }else {
                size_t tmp_entry;
                obj=getValueEntry(k,tmp_entry);
                //std::cout<<"insert entry data offset "<<tmp<<std::endl;
                if(NULL==obj) {
                    HashBucket bucket;
                    bucket.header=tmp;
                    if(STO_OK!=hashBucket_->InsertAndUpdateData(bucket,offset)) {
                        //std::cout<<"insert and update bucket failed!"<<std::endl;
                        return -1;
                    }
                    //std::cout<<"upinsert bucket offset "<<offset<<" header "<<tmp<<std::endl;
                    return 0;
                }

                const ENTRY* pre=NULL;
                size_t tmp_pos=tmp_entry;
                while(obj!=NULL) {
                    pre=obj;
                    if(obj->next!=SIZE_MAX) {
                        tmp_pos=obj->next;
                        obj=hashValue_->FindDataPtr(obj->next);
                    }else {
                        break;
                    }
                }
                if(NULL!=pre) {
                    ENTRY hve;
                    hve=*pre;
                    hve.next = tmp;
                    hashValue_->UpdateData(hve,tmp_pos);
                    //std::cout<<"link "<<tmp_pos<<" -> "<<tmp<<std::endl;
                }
            }
        } else {
            bool repeat=false;
            for(size_t i=0;i<obj->item_num && i<sizeof(obj->item)/sizeof(obj->item[0]);i++) {
                if(obj->item[i].offset==obj_offset) {
                    repeat=true;
                    continue;
                }
            }
            if(repeat) {
                //std::cout<<"repeat key="<<k<<" offset="<<obj_offset<<std::endl;
                return 1;
            }
            size_t len=sizeof(obj->item)/sizeof(obj->item[0]);
            ENTRY hve=*obj;
            bool change=false;
            for(int i=obj->item_num-1;i>=0;i--) {
                if(hve.item[i].score<score || (0==score && hve.item[i].score==score)) {
                    change=true;
                    if(i<len-1) {
                        hve.item[i+1]=hve.item[i];
                    }
                    if(i==0) {
                        hve.item[0].offset=obj_offset;
                        hve.item[0].score = score;
                        break;
                    }
                }else{
                    if(i!=len-1){
                        hve.item[i+1].offset=obj_offset;
                        hve.item[i+1].score = score;
                        if(i==obj->item_num-1) {
                            change=true;
                        }
                    }

                    break;
                }
            }
            if(change) {
                if(hve.item_num < len) {
                    hve.item_num++;
                }
                hashValue_->UpdateData(hve,entry_offset);
            }
        }
        return 0;
    }

    inline int del(const string &key) {
        size_t offset = getBucket(key);
        const HashBucket* b=hashBucket_->FindDataPtr(offset);
        if(NULL==b) {
            return -1;
        }
        const ENTRY* v=hashValue_->FindDataPtr(b->header);
        const ENTRY* pre=NULL;
        const ENTRY* after=NULL;
        size_t cur_pos=b->header;
        size_t pre_offset=SIZE_MAX;
        size_t after_offset=SIZE_MAX;
        while(cur_pos!=SIZE_MAX && v!=NULL) {
            if(v->term == key) {
                //找到after元素
                if(v->next==SIZE_MAX) {
                    after=NULL;
                    after_offset=SIZE_MAX;
                }else {
                    after=hashValue_->FindDataPtr(v->next);
                    after_offset=v->next;
                }
                if(pre==NULL && after==NULL) {
                    //没有hash冲突时，直接删除当前元素，并删除bucket
                    hashValue_->DeleteData(cur_pos);
                    hashBucket_->DeleteData(offset);
                }
                else if(pre==NULL && after!=NULL) {
                    //有hash冲突，删除的是第一个元素时,更新bucket header指向,删除当前元素
                    hashValue_->DeleteData(cur_pos);
                    HashBucket hb;
                    hb.header=after_offset;
                    hashBucket_->UpdateData(hb,offset);
                }else if(pre!=NULL) {
                    //有hash冲突，并且删除的不是第一个元素时,更新pre->next到after
                    ENTRY entry=*pre;
                    entry.next=after_offset;
                    hashValue_->UpdateData(entry,pre_offset);
                }
                return 0;
            }
            pre=v;
            pre_offset=cur_pos;
            cur_pos=v->next;
            v=hashValue_->FindDataPtr(v->next);
        }
        return 1;
    }

    float getLoadFactor() const {
        size_t bucket_len=bucketSize_;
        size_t hash_size=hashSize();
        return float(hash_size)/float(bucket_len);
    }

    void printStatus() const{
        size_t bucket_len=bucketSize_;
        size_t hash_size=hashSize();
        std::cout<<"######################shared hash map status#########################"<<std::endl;
        std::cout<<"Load factor="<<float(hash_size)/float(bucket_len)<<std::endl;
        for(size_t i=0;i<bucket_len;i++) {
            const HashBucket* b=hashBucket_->FindDataPtr(i);
            if(NULL==b) {
                continue;
            }
            std::cout<<"bucket offset "<< i <<" entry : ";
            size_t cur_pos=b->header;
            const ENTRY* v=hashValue_->FindDataPtr(b->header);
            std::set<size_t> offsets;
            while(v!=NULL) {
                std::cout<<"->"<<cur_pos<<","<<v->term<<","<<v->next;
                cur_pos=v->next;
                if(offsets.find(cur_pos)!=offsets.end()) {
                    std::cout<<" error found circle link ,continue";
                    break;
                }
                offsets.insert(cur_pos);
                for(size_t j=0;j<v->item_num;j++) {
                    std::cout<<"|"<<v->item[j].offset;
                }
                if(v->next!=SIZE_MAX) {
                    v=hashValue_->FindDataPtr(v->next);
                }else {
                    v=NULL;
                }
            }
            std::cout<<std::endl;
        }
        std::cout<<"####################################################################"<<std::endl;
    }

    inline DocResult<V> get(const string key) const {
        size_t offset;
        DocResult<V> dr;
        const ENTRY *value= getValue(key,offset);
        if(NULL==value) {
             //std::cout<<"value is null"<<std::endl;;
            return dr;
        }
        for(size_t i=0;i<value->item_num;i++) {
            //std::cout<<"doc offset="<<value->item[i].offset<<std::endl;
            const V* v=docData_->FindDataPtr(value->item[i].offset);
            if(NULL!=v) {
                DocValue<V> dv;
                dv.doc = v;
                dv.score = value->item[i].score;
                dr.docs.push_back(dv);
            }
        }
        return dr;
    }
};
}
#endif
