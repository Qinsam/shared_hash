#ifndef SHARED_HASH_FUN_H
#define SHARED_HASH_FUN_H

#include <stddef.h>
#include "type.h"

namespace shm{
/*
static size_t hash_code(String &value) {
	int32_t hashCode = 0;
	for (String::const_iterator ptr = value.begin(); ptr != value.end(); ++ptr) {
		hashCode = hashCode * 31 + *ptr;

	}
	return hashCode>=0?hashCode:-hashCode;
}
*/
static size_t hash_code(const string &value) {
	int32_t hashCode = 0;
	for (string::const_iterator ptr = value.cbegin(); ptr != value.cend(); ++ptr) {
		hashCode = hashCode * 31 + *ptr;
	}
	return hashCode>=0?hashCode:-hashCode;
}

}

#endif
