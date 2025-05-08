#ifdef __APPLE__
/* This provides a redefinition-safe fdopen on macOS as zlib is incompatible without it */
#include <stdio.h>

FILE* my_fdopen_wrapper(int fd, const char* mode) {
    return fdopen(fd, mode);
}
#endif