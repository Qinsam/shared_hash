#ifndef SHARED_HASH_SET_H
#define SHARED_HASH_SET_H

#include <iostream>
#include <set>
#include "type.h"
#include "./basemmap/include/DataStorage.hpp"
#include "shared_hash_fun.h"

/*基于mmap的hash_set实现，key只支持string,其他类型通过转换为唯一string来存储
 *通过HASH_SET_CONF(entry_name,max_query_len)宏来生成一个配置，
 *第一个参数是hash_entry的name，可以自定义随便起,多个的话不能重复
 *第二个参数是max_query_le,代表支持的query的最大长度
  *
 *使用示例:

 *HASH_SET_CONF(test,50);
 *
 *string path="/home/test/mmap";
 *size_t bucket_num=10000000;
 *SharedHashSet<test> *shs=new SharedHashSet<test>(path,bucket_num);  //bucket_num可以省略，默认是10000000,
 *shs->Init();
 *
 *
 *
 *string key="test";
 *shs->insert(key);
 *
 *查询方法：
 *bool ret=shs->has(key);
 *
 *实现原理:
 *
 *
 *bucket   OOOOOOOOOOO     ##bucket 个数从创建后不发生改变,开始时尽量开大一点，可以减少hash冲突，
 *             \           ## 加快访问速度,后续可能加入rehash功能
 *              \
 *               \
 *hash_entry OOOOOOOOOOOOOOOOOOOOOOOOOOOOOO   ##value in bucket，自动扩容
 *
 */

namespace shm{

#define HASH_SET_CONF(entry_name,max_query_len) \
struct entry_name {                   \
	size_t next;                      \
    char term[max_query_len];         \
	entry_name() {                    \
		next=SIZE_MAX;                \
	}                                 \
}

template<typename ENTRY>
class SharedHashSet {
private:
	CDataStorage<HashBucket> *hashBucket_; //hash数据入口
	CDataStorage<ENTRY> *hashValue_;  //hash数据
    ENTRY en;

public:
	SharedHashSet(string &datapath,CModeType m=M_READWRITE,
					  size_t bucket_num=10000000) {
		string bkdatafile=datapath+"/bucket.data";
		string bkbitfile = datapath+"/bucket.bit";
		string hmdatafile=datapath+"/value.data";
		string hmbitfile=datapath+"/value.bit";
        hashBucket_ = new CDataStorage<HashBucket>(bkdatafile,bkbitfile,bucket_num,m,2);
		hashValue_ = new CDataStorage<ENTRY>(hmdatafile,hmbitfile,bucket_num*3,m);
	}

	~SharedHashSet() {
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
		if(!hashBucket_->Init()) {
			std::cout<<"init hash bucket failed!"<<std::endl;
			return false;
		}
		if(!hashValue_->Init()) {
			std::cout<<"init hash value failed!"<<std::endl;
			return false;
		}
		return true;
	}

	inline bool empty() const {
		return 0==hashBucket_->GetStorageItemCount();
	}

	inline size_t hashSize() const {
		return hashValue_->GetStorageItemCount();
	}

	inline size_t bucketSize() const {
		return hashBucket_->GetItemCapacity();
	}

	inline const ENTRY* getValue(const string &k,size_t &entry_offset) const{
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
            //std::cout<<"v->term="<<v->term<<" k="<<k<<std::endl;
			if(v->term==k) {
                //std::cout<<" get value found value offset "<<entry_offset<<std::endl;
				break;
            }else {
                //std::cout<<"v->term="<<v->term<<" k="<<k<<" not equel"<<std::endl;
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

	inline const ENTRY* getValueEntry(const string &k,size_t &entry_offset) const{
      size_t offset = getBucket(k);
		const HashBucket* b=hashBucket_->FindDataPtr(offset);
        if(NULL==b) {
            return NULL;
        }
		const ENTRY* v=hashValue_->FindDataPtr(b->header);
        entry_offset=b->header;
		return v;
	}

	inline size_t getBucket(const string &k) const{
		size_t hashCode = hash_code(k);
		return hashCode % bucketSize();
	}

	inline int insert(const string &k) {
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
			if(STO_OK!=hashValue_->InsertData(entry,tmp)) {
				//std::cout<<"insert  entry failed!"<<std::endl;
				return -1;
			}else {
                //std::cout<<"insert pos"<<tmp<<std::endl;
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
			//key已经存在，直接不做插入
			return 0;
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
                //std::cout<<"del pos "<<cur_pos<<std::endl;
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
        size_t bucket_len=bucketSize();
        size_t hash_size=hashSize();
        return float(hash_size)/float(bucket_len);
    }

    void printOneStatus(const string &k) const {
		size_t bucket_len=bucketSize();
		size_t hash_size=hashSize();
        std::cout<<"######################shared hash set one status#########################"<<std::endl;
		std::cout<<"Load factor="<<float(hash_size)/float(bucket_len)<<std::endl;
        size_t i=getBucket(k);
        std::cout<<"key = "<<k<<std::endl;
		const HashBucket* b=hashBucket_->FindDataPtr(i);
		if(NULL==b) {
            std::cout<<"no bucket "<<i<<std::endl;
			return;
		}
		std::cout<<"bucket offset "<< i <<" entry : ";
		size_t cur_pos=b->header;
		const ENTRY* v=hashValue_->FindDataPtr(b->header);
        std::set<size_t> offsets;
		while(v!=NULL) {
			std::cout<<"->"<<cur_pos<<","<<v->term<<","<<v->next;
            cur_pos=v->next;
            if(offsets.find(cur_pos)!=offsets.end()) {
                std::cout<<"error found circle link";
                break;
            }
            offsets.insert(cur_pos);
			if(v->next!=SIZE_MAX) {
				v=hashValue_->FindDataPtr(v->next);
			}else {
				v=NULL;
			}
		}
		std::cout<<std::endl;
        std::cout<<"#####################################################################"<<std::endl;
    }

    void printStatus() const{
		size_t bucket_len=bucketSize();
		size_t hash_size=hashSize();
        std::cout<<"######################shared hash set status#########################"<<std::endl;
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
				if(v->next!=SIZE_MAX) {
					v=hashValue_->FindDataPtr(v->next);
				}else {
					v=NULL;
				}
			}
			std::cout<<std::endl;
		}
        std::cout<<"#####################################################################"<<std::endl;
    }

	inline bool has(const string &key) const{
        size_t offset;
        const ENTRY *value= getValue(key,offset);
		if(NULL==value) {
			return false;
		}
        return true;
    }
};
}
#endif
