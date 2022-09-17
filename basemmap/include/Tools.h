#ifndef _H_TOOLS_H__
#define _H_TOOLS_H__

#include <string.h>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <libgen.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using std::string;

namespace shm{

bool IsExistFile( const string& filename );
bool MakeDir( const string& filename );

#ifndef SIZE_MAX
#define SIZE_MAX (18446744073709551615u)
#endif

}

#endif
