// Just the data types that can be stored in my db
// DT : data type.

#pragma once

#include "main_lib.h"
#include <uuid/uuid.h>

// CORE DATA TYPES :

typedef enum SIMPL_DT_LIST { 
    SIMPL_INT_DT = 0,
    SIMPL_STR_DT = 1,
    SIMPL_FLOAT_DT = 2,
    SIMPL_DOUBLE_DT = 3,
    SIMPL_BOOL_DT = 4,
    SIMPL_DATETIME_DT = 5,
    SIMPL_BINARY_DATA_DT = 6,
    SIMPL_UUID_DT = 7
} SIMPL_DT_LIST;

typedef struct SIMPL_INT {
    int64_t val;
} SIMPL_INT;

typedef struct SIMPL_FLOAT { 
    float val;
} SIMPL_FLOAT;

typedef struct SIMPL_DOUBLE { 
    double val;
} SIMPL_DOUBLE;

typedef struct SIMPL_STR { 
    char *val;
} SIMPL_STR;

typedef struct SIMPL_BOOL {
    bool val;
} SIMPL_BOOL;
 
// CUSTOM DATA TYPES:

typedef struct SIMPL_DATETIME_INFO {
    SIMPL_INT min_year, max_year;
    bool am_pm;
} SIMPL_DATETIME_INFO;

typedef struct SIMPL_DATETIME {
    SIMPL_INT year, month, day, hour, minute, second;
} SIMPL_DATETIME;

typedef struct SIMPL_UUID {
    uuid_t raw_uuid;
    SIMPL_STR uuid_str;
} SIMPL_UUID;

typedef struct __attribute__((packed)) SIMPL_BINARY_DATA { 
    uint16_t header;
    uint16_t length_by_bytes;
    uint8_t *data;
} SIMPL_BINARY_DATA;
