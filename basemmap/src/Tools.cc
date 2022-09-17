#include "Tools.h"

namespace shm {

bool IsExistFile( const string& filename ){
    if (access(filename.c_str(), F_OK) == -1)
        return false;
    return true;
}

bool MakeDir( const string& filename ){
    if ( filename.size() == 0 ){
        return false;
    }
    char *dir_dup = (char*)strdup(filename.c_str());
    char *base_dup = (char*)strdup(filename.c_str());
    char *dir_name = dirname(dir_dup);
    if (access(dir_name, F_OK) == 0) {
        free( base_dup );
        free( dir_dup );
        return true;
    }
    umask(0);
    for (size_t i = 1; i < strlen(dir_name); ++i) {
        if (dir_name[i] == '/') {
            dir_name[i] = 0;
            if (access(dir_name, F_OK) != 0 && mkdir(dir_name, 0760) == -1) {
                free( base_dup );
                free( dir_dup );
                return false;
            }
            dir_name[i] = '/';
        }
    }
    //lastdir
    mkdir(dir_name, 0760);
    free( base_dup );
    free( dir_dup );
    return true;
}
}
