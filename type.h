#ifndef SHARED_HASH_TYPE_H
#define SHARED_HASH_TYPE_H

namespace shm{

typedef std::basic_string< wchar_t, std::char_traits<wchar_t> > String;
typedef std::basic_ostringstream< wchar_t, std::char_traits<wchar_t> > StringStream;

const std::basic_string< wchar_t, std::char_traits<wchar_t> > EmptyString;

struct HashBucket {
	size_t header;//指向hash数据头
	HashBucket() {
		header=0;
	}
};

}
#endif
