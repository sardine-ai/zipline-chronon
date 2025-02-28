namespace py ai.chronon.common
namespace java ai.chronon.api

// integers map to milliseconds in the timeunit
enum TimeUnit {
    HOURS = 0
    DAYS = 1
    MINUTES = 2
}

struct Window {
    1: i32 length
    2: TimeUnit timeUnit
}

enum ConfigType {
    STAGING_QUERY = 1
    GROUP_BY = 2
    JOIN = 3
    MODEL = 4
}