package(default_visibility = ["//visibility:public"])

# Import config_setting for macOS
config_setting(
    name = "macos",
    constraint_values = ["@platforms//os:macos"],
)

# Define zlib with conditional compilation flags
cc_library(
    name = "zlib",
    srcs = glob(["*.c"], exclude = ["example.c", "minigzip.c"]),
    hdrs = glob(["*.h"]),
    includes = ["."],
    defines = select({
        ":macos": [
            "HAVE_UNISTD_H",
            "NO_FSEEKO",
            "HAVE_STDARG_H",
            "_DARWIN_C_SOURCE",
            "fdopen=my_fdopen_wrapper", # Redefine fdopen to avoid conflicts
        ],
        "//conditions:default": [],
    }),
    copts = select({
        ":macos": [
            "-Wno-macro-redefined",
            "-Wno-deprecated-non-prototype",
        ],
        "//conditions:default": [],
    }),
)
