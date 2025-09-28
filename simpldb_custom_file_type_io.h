/*
 
-> This lib is for the my custom ".simpldb" file type.
-> I wanted make this because simpldb should be my version of sql which means it must have it's data type.

-> Actually this is a little bit useless because you can already get your database as 
   csv file which is better for future use (like using the csv file with python pandas lib).

-> But this is little bit useful too because csv just stores everything as strings this custom 
   file type will store everything as their original.

-> As you can see this a very good exercise because i will just do binary operations in a file.
-> This wont be like a json file i will store some raw binary in that file.

*/

#include "db_custom_data_types.h"
#include "db_dtype_operations.h"
#include "simpldb_core.h"
#include "main_lib.h"
#include <uuid/uuid.h>

// Random magic numbers:

// File header thing.
#define SIMPLDB_MAGIC 0xFCFCFCFC
#define SDBINFO_MAGIC 0xCFCFCFCF

// MODE_READ for reading raw binary MODE_ANALYZE is reading macros.
#define MODE_READ    0x01
#define MODE_ANALYZE 0x00

// These bad boys will help me read the page metadatas.
#define PAGE_START              0xFFFF0000
#define PAGE_END                0xFFFF0001
#define TOTAL_PAGES             0xFFFF0002
#define PAGE_START              0xFFFF0003
#define PAGE_END                0xFFFF0004

// Data types.
#define SIMPL_INT_T      0x00
#define SIMPL_STR_T      0x01
#define SIMPL_UUID_T     0x02
#define SIMPL_DATETIME_T 0x03
#define SIMPL_BIN_T      0x04
#define SIMPL_FLOAT_T    0x05
#define SIMPL_DOUBLE_T   0x06
#define SIMPL_BOOL_T     0x07
#define DATA_END         0x08
#define ERR              0x09

// For writing column data.
#define COL_NAME_START  0x0000FFFF
#define COL_NAME_SIZE   0x0001FFFF
#define COL_INDEX_START 0x0002FFFF

// For writing row metadatas
#define ROW_MT_START 0xF0000000
#define ROW_MT_END   0xF0001000

// For writing db_data metadatas.
#define DB_DATA_START 0xD0000000
#define DB_DATA_END   0xD0000001

// Some useful macros.
#define ROW_WRITTEN_EACH_PAGE db->columns.col_count
#define FILE_EXTENSION ".simpldb"
#define INFO_FILE_EXTENSION ".sdbinfo"
#define FILE_EXT_LENGTH 8 // len(".simpldb")
#define INFO_FILE_EXT_LENGTH 8 // len(".sdbinfo")
#define WRITE_CURRENT_MAGIC fwrite(magic_num, sizeof(uint32_t), 1, simpldb);
#define READ_TO_MAGIC__simpldb fread(magic_num, sizeof(uint32_t), 1, simpldb);
#define READ_TO_MAGIC__sdbinfo fread(magic_num, sizeof(uint32_t), 1, sdbinfo);
#define CHECK_ERRORS if(status) {fprintf(stderr, "Can't read file -> read_simpldb");kill_db(db);return NULL;}

#pragma once

// Returns file name with sdbinfo or simpldb extension.
static char* get_fname_str(const char *fname, bool ext_type) {
    uint16_t length;
    for(length = 0; *(fname + length) != '\0'; length++);
    char *str = (char*) malloc((length + 1 + 8)*sizeof(char));

    for(uint16_t i = 0; i < length; i++) {
        str[i] = fname[i];
    }

    if(ext_type) {
        str[length] = '.';
        str[length + 1] = 's';
        str[length + 2] = 'i';
        str[length + 3] = 'm';
        str[length + 4] = 'p';
        str[length + 5] = 'l';
        str[length + 6] = 'd';
        str[length + 7] = 'b';
    }
    else {
        str[length] = '.';
        str[length + 1] = 's';
        str[length + 2] = 'd';
        str[length + 3] = 'b';
        str[length + 4] = 'i';
        str[length + 5] = 'n';
        str[length + 6] = 'f';
        str[length + 7] = 'o';
    }

    str[length + 8] = '\0';

    return str;
}

// Returns file name without extension. 
static char* return_raw_fname(const char *fname) {
    uint16_t length;
    for(length = 0; *(fname + length) != '\0'; length++);
    length -= 8;

    char *str = (char*)malloc((length + 1)*sizeof(char));

    for(uint16_t i = 0; i < length; i++) {
        str[i] = fname[i];
    } 

    str[length + 1] = '\0';
    return str;
}

// Check if file exists.
static bool check_file(const char *fname) {
    FILE *fptr = fopen(fname, "r");
    if(fptr != NULL) {
        fclose(fptr);
        return true;
    }
    return false;
}

// Gets data type return 8bit val of data type.
static uint8_t get_dt(ROW *r, uint16_t index) {
    switch(r->datas[index].dtype) {
        case SIMPL_BINARY_DATA_DT:
            return SIMPL_BIN_T;
            break;
        case SIMPL_STR_DT:
            return SIMPL_STR_T;
            break;
        case SIMPL_BOOL_DT:
            return SIMPL_BOOL_T;
            break;
        case SIMPL_INT_DT:
            return SIMPL_INT_T;
            break;
        case SIMPL_DATETIME_DT:
            return SIMPL_DATETIME_T;
            break;
        case SIMPL_DOUBLE_DT:
            return SIMPL_DOUBLE_T;
            break;
        case SIMPL_FLOAT_DT:
            return SIMPL_FLOAT_T;
            break;
        case SIMPL_UUID_DT:
            return SIMPL_UUID_T;
            break;
        default:
            return ERR;
            break;
    }
    return ERR;
}

// Used for writing data in pages.
static void write_SIMPL_BINARY_DATA(FILE *fptr, SIMPL_BINARY_DATA *sbin) {
    // Order will be : header -> length variable -> data
    fwrite(&sbin->header, sizeof(uint16_t), 1, fptr); 
    fwrite(&sbin->length_by_bytes, sizeof(uint16_t), 1, fptr); 
    fwrite(sbin->data, (size_t)sbin->length_by_bytes, 1, fptr); 
}

static void write_SIMPL_STR(FILE *fptr, SIMPL_STR *str) {
    uint32_t length;
    for(length = 0; *(str->val + length) != '\0'; length++);
    fwrite(str->val, length, 1, fptr);
}

static void write_SIMPL_BOOL(FILE *fptr, SIMPL_BOOL *sb) {
    fwrite(&sb->val, sizeof(bool), 1, fptr);
}

static void write_SIMPL_INT(FILE *fptr, SIMPL_INT *s64) {
    fwrite(&s64->val, sizeof(int64_t), 1, fptr);
}

static void write_SIMPL_DATETIME(FILE *fptr, SIMPL_DATETIME *sdate) {
    // Order will be : year, month, day, hour, minute, second.
    write_SIMPL_INT(fptr, &sdate->year);
    write_SIMPL_INT(fptr, &sdate->month);
    write_SIMPL_INT(fptr, &sdate->day);
    write_SIMPL_INT(fptr, &sdate->hour);
    write_SIMPL_INT(fptr, &sdate->minute);
    write_SIMPL_INT(fptr, &sdate->second);
}

static void write_SIMPL_DOUBLE(FILE *fptr, SIMPL_DOUBLE *sd) {
    fwrite(&sd->val, sizeof(double), 1, fptr);
}

static void write_SIMPL_FLOAT(FILE *fptr, SIMPL_FLOAT *sf) {
    fwrite(&sf->val, sizeof(double), 1, fptr);
}

// Might have errors because of uuid_t behaviour.
static void write_SIMPL_UUID(FILE *fptr, SIMPL_UUID *suuid) {
    write_SIMPL_STR(fptr, suuid->uuid_str.val);
    fwrite(suuid->raw_uuid, sizeof(uuid_t), 1, fptr); // ERROR PROBABLY.
}

static uint8_t compare_magic(uint32_t wanted_magic_num, uint32_t *val) {
    if(wanted_magic_num != val[0]) {
        return 1;
    }
    return 0;
}

static SIMPL_DT_LIST read_dt(uint8_t dt) {
    switch(dt) {
        case SIMPL_BIN_T:
            return SIMPL_BINARY_DATA_DT;
        case SIMPL_BOOL_T:
            return SIMPL_BOOL_DT;
        case SIMPL_DATETIME_T:
            return SIMPL_DATETIME_DT;
        case SIMPL_UUID_T:
            return SIMPL_UUID_DT;
        case SIMPL_FLOAT_T:
            return SIMPL_FLOAT_DT;
        case SIMPL_DOUBLE_T:
            return SIMPL_DOUBLE_DT;
        case SIMPL_STR_T:
            return SIMPL_STR_DT;
        case SIMPL_INT_T:
        case ERR:
            return SIMPL_INT_DT;

    }
    return SIMPL_INT_DT;
}

/*
    :::FILE DESIGN:::
    -----------------

    .SDBINFO:
    
    #SDBINFO_MAGIC

    #TOTAL_PAGES_MAGIC
    -> counts the total pages and writes here.

    -> row count basically same with total pages accidentally put this varaible and i dont wanna delete it
    
    -> writes col count.

    ->for each column:
        #COL_NAME_SIZE_MAGIC
        -> writes the size of column name size.

        #COL_INDEX_MAGIC
        -> writes the size of the column index value.

        #COL_NAME_MAGIC
        -> writes the column name.

    ----------------------------------

    .SIMPLDB:
    
    #SIMPLDB_MAGIC

    -> for each row:
        
        #PAGE_START_MAGIC
        
        #ROW_MT_START_MAGIC // mt: metadata
        -> number_of_datas variable.
        -> pos variable.
        #ROW_MT_END_MAGIC

        ->for each data:
            
            #DB_DATA_START_MAGIC
            #DATA_TYPE_MAGIC
            -> pos variable.

            -> if bin write header.
            -> if str or bin write size.

            -> data.
        #PAGE_END_MAGIC

    ---------------------------------------------------------------------------
*/


// Reads simpldb file.
DATABASE* read_simpldb(const char *fname) {
    if(fname == NULL) {
        fprintf(stderr, "Please give valid parameters -> read_simpldb.\n");
        return NULL;
    }
    else if(check_file(fname) == false) {
        fprintf(stderr, "Please give an existing file -> read_simpldb.\n");
        return NULL;
    }
    
    DATABASE *db = create_db();

    if(db == NULL) {
        fprintf(stderr, "Can't allocate memory -> read_simpldb.\n");
        return NULL;
    }


    char *fname_handler = return_raw_fname(fname);
    char *handler_ptr = fname_handler;

    fname_handler = get_fname_str(fname_handler, false);
    
    FILE *sdbinfo = fopen(fname_handler, "rb");

    free(fname_handler);
    fname_handler = NULL;
    fname_handler = handler_ptr;
    fname_handler = get_fname_str(fname_handler, true); 

    FILE *simpldb = fopen(fname_handler, "rb");

    free(fname_handler);
    free(handler_ptr);

    fname_handler = NULL;
    handler_ptr = NULL;

    if(simpldb == NULL || sdbinfo == NULL) {
        fprintf(stderr, "Can't open file -> read_simpldb.\n");
        return NULL;
    }

    uint8_t status = 0;
    uint32_t *magic_num =(uint32_t*)malloc(sizeof(uint32_t));
    
    // .sdbinfo   
    // Reads data and comapares magic numbers if magic numbers are false returns null.

    // File magic number.
    READ_TO_MAGIC__sdbinfo
    status = (*magic_num == SDBINFO_MAGIC);
    CHECK_ERRORS

    // Total page magic number.
    READ_TO_MAGIC__sdbinfo
    status = (*magic_num == TOTAL_PAGES);
    CHECK_ERRORS

    // Reads row count.
    READ_TO_MAGIC__sdbinfo
    db->rows.row_count = magic_num[0];

    // Reads row count. USELESS added this accidentally.
    READ_TO_MAGIC__sdbinfo
    db->rows.row_count = magic_num[0];
        
    // Reads column count.
    READ_TO_MAGIC__sdbinfo
    db->columns.col_count = magic_num[0];

    SIMPL_STR *names = (SIMPL_STR*)malloc(db->columns.col_count*sizeof(SIMPL_STR));
    uint32_t size;

    for(uint16_t i = 0; i < db->columns.col_count; i++) {
        READ_TO_MAGIC__sdbinfo
        status = (*magic_num == COL_NAME_SIZE);
        
        names[i].val = (char*)malloc(magic_num[0]*sizeof(char));
        size = magic_num[0];

        READ_TO_MAGIC__sdbinfo
        status = (*magic_num == COL_INDEX_START);
        CHECK_ERRORS

        db->columns.columns[i].col_index = (uint16_t)magic_num[0];

        READ_TO_MAGIC__sdbinfo
        status = (*magic_num == COL_NAME_START); 
        fread(names[i].val, 1, size, sdbinfo);
    }

    // Adding all columns.
    add_column(db, names, true);
    
    // Freeing column names array struct.
    for(uint16_t i = 0; i < db->columns.col_count; i++) {
        free(names->val);
    }
    free(names);

    // Closing sdbinfo file.
    fclose(sdbinfo);

    // Allocating rows using row count
    
    create_row(db, 255); // anytning between 1-254 will do the same thing.

    // .simpldb
    // Reads data and compares the magic numbers.
    READ_TO_MAGIC__simpldb
    status = (*magic_num == SIMPLDB_MAGIC);
    CHECK_ERRORS

    DB_DATA *datas = (DB_DATA*)malloc(db->columns.col_count*sizeof(DB_DATA));

    for(uint16_t i = 0; i < db->rows.row_count; i++) {
        READ_TO_MAGIC__simpldb
        status = (*magic_num == PAGE_START);
        CHECK_ERRORS

        READ_TO_MAGIC__simpldb
        status = (*magic_num == ROW_MT_START);
        CHECK_ERRORS
        
        READ_TO_MAGIC__simpldb
        db->rows.rows[i].number_of_datas = (uint16_t)magic_num[0]; 
        
        size = magic_num[0];

        READ_TO_MAGIC__simpldb
        db->rows.rows[i].pos = (uint16_t)magic_num[0];
        
        READ_TO_MAGIC__simpldb
        status = (*magic_num == ROW_MT_END);
        CHECK_ERRORS

        for(uint16_t j = 0; j < size; j++) {
            READ_TO_MAGIC__simpldb
            status = (*magic_num == DB_DATA_START);
            CHECK_ERRORS
            
            uint8_t dtype;
            fread(&dtype, sizeof(uint8_t), 1, simpldb);
            SIMPL_DT_LIST tmp = read_dt(dtype);
            datas[i].dtype = tmp;

            READ_TO_MAGIC__simpldb
            datas[i].pos = (uint16_t)magic_num[0];
            
            if(datas[i].dtype == SIMPL_BINARY_DATA_DT) {
                READ_TO_MAGIC__simpldb
                datas[i].data.bin.header = (uint16_t)magic_num[0];
                READ_TO_MAGIC__simpldb
                datas[i].data.bin.length_by_bytes = (uint16_t)magic_num[0];
            }
            else if(datas[i].dtype == SIMPL_STR_DT) {
                READ_TO_MAGIC__simpldb
                size = magic_num[0];
            }

            switch(datas[i].dtype) {
                case SIMPL_BINARY_DATA_DT:
                    fread(datas[i].data.bin.data, sizeof(uint8_t), size, simpldb);
                    break;
                case SIMPL_STR_DT:
                    fread(datas[i].data.ss.val, sizeof(char), size, simpldb);
                    break;
                case SIMPL_DATETIME_DT:
                    fread(&datas[i].data.sdate.year.val, sizeof(int64_t), 1, simpldb);
                    fread(&datas[i].data.sdate.month, sizeof(int64_t), 1, simpldb);
                    fread(&datas[i].data.sdate.day, sizeof(int64_t), 1, simpldb);
                    fread(&datas[i].data.sdate.hour, sizeof(int64_t), 1, simpldb);
                    fread(&datas[i].data.sdate.minute, sizeof(int64_t), 1, simpldb);
                    fread(&datas[i].data.sdate.second, sizeof(int64_t), 1, simpldb);
                    break;
                case SIMPL_FLOAT_DT:
                    fread(&datas[i].data.sf.val, sizeof(float), 1, simpldb);
                    break;
                case SIMPL_DOUBLE_DT:
                    fread(&datas[i].data.sd.val, sizeof(double), 1, simpldb);
                    break;
                case SIMPL_UUID_DT:
                    fread(datas[i].data.suuid.uuid_str.val, sizeof(char), UUID_LENGTH, simpldb);
                    fread(datas[i].data.suuid.raw_uuid, sizeof(uuid_t), 1, simpldb); // PROBABLY ERROR.
                    break;
                case SIMPL_BOOL_DT:
                    fread(&datas[i].data.sb.val, sizeof(bool), 1, simpldb);
                    break;
                case SIMPL_INT_DT:
                    fread(&datas[i].data.si.val, sizeof(int64_t), 1, simpldb);
                    break;
            }
        }

        READ_TO_MAGIC__simpldb
        status = (*magic_num == PAGE_END);
        CHECK_ERRORS

    }
    
    free(datas->data.bin.data);
    free(datas->data.ss.val);
    free(datas->data.suuid.raw_uuid);
    free(datas->data.suuid.uuid_str.val);
    free(datas);
    free(magic_num);
}

// Exports simpldb info file.
static void export_simpldb_info(const char *fname, DATABASE *db) {
    char *fname_w_ext = get_fname_str(fname, false);
    FILE *simpldb = fopen(fname_w_ext, "wb");
    free(fname_w_ext);
    fname_w_ext = NULL;

    if(simpldb == NULL) {
        fclose(simpldb);
        fprintf(stderr, "Can't create a file -> export_simpldb.\n");
        return;
    }

    uint32_t *magic_num = (uint32_t*)malloc(sizeof(uint32_t));

    // Start of file using simpldb magic num.
    magic_num[0] = SDBINFO_MAGIC;
    WRITE_CURRENT_MAGIC

    // Calculate total pages and write.
    magic_num[0] = TOTAL_PAGES;
    WRITE_CURRENT_MAGIC
    magic_num[0] = (uint32_t)db->rows.row_count;
    WRITE_CURRENT_MAGIC

    // Row count info.
    magic_num[0] = db->rows.row_count;
    WRITE_CURRENT_MAGIC

    // Write columns.
    magic_num[0] = db->columns.col_count;
    WRITE_CURRENT_MAGIC

    for(uint16_t i = 0; i < db->columns.col_count; i++) {
        uint16_t j;

        magic_num[0] = COL_NAME_SIZE;
        WRITE_CURRENT_MAGIC
        for(j = 0; *(db->columns.columns[i].col_name.val + j) != '\0'; j++);
        magic_num[0] = j + 1;
        WRITE_CURRENT_MAGIC

        magic_num[0] = COL_INDEX_START;
        WRITE_CURRENT_MAGIC
        magic_num[0] = db->columns.columns[i].col_index;

        magic_num[0] = COL_NAME_START;
        WRITE_CURRENT_MAGIC
        fwrite(db->columns.columns[i].col_name.val, sizeof(j + 1), 1, simpldb);
    }

    fclose(simpldb);
    simpldb = NULL;

    free(magic_num);
    magic_num = NULL;
}

static void export_simpldb_data(const char *fname, DATABASE *db) {
    char *fname_w_ext = get_fname_str(fname, true);
    FILE *simpldb = fopen(fname_w_ext, "wb");
    free(fname_w_ext);
    fname_w_ext = NULL;

    if(simpldb == NULL) {
        fclose(simpldb);
        fprintf(stderr, "Can't create a file -> export_simpldb.\n");
        return;
    }

    uint32_t *magic_num = (uint32_t*)malloc(sizeof(uint32_t));

    magic_num[0] = SIMPLDB_MAGIC;
    WRITE_CURRENT_MAGIC

    for(uint16_t i = 0; i < db->rows.row_count; i++) {
        magic_num[0] = PAGE_START;
        WRITE_CURRENT_MAGIC

        magic_num[0] = ROW_MT_START;
        WRITE_CURRENT_MAGIC

        magic_num[0] = db->rows.rows[i].number_of_datas;
        WRITE_CURRENT_MAGIC

        magic_num[0] = db->rows.rows[i].pos;
        WRITE_CURRENT_MAGIC 

        magic_num[0] = ROW_MT_END;
        WRITE_CURRENT_MAGIC

        for(uint16_t j = 0; j < db->rows.rows[i].number_of_datas; j++) {
            magic_num[0] = DB_DATA_START;
            WRITE_CURRENT_MAGIC     
            
            uint8_t dt = get_dt(&(db->rows.rows[i]), j);
            fwrite(&dt, sizeof(uint8_t), 1, simpldb);
            
            magic_num[0] = db->rows.rows[i].datas[j].pos;
            WRITE_CURRENT_MAGIC

            switch(db->rows.rows[i].datas[j].dtype) {
                case SIMPL_INT_DT:
                    write_SIMPL_INT(simpldb, &db->rows.rows[i].datas[j].data.si);
                    break;
                case SIMPL_FLOAT_DT:
                    write_SIMPL_FLOAT(simpldb, &db->rows.rows[i].datas[j].data.sf);
                    break;
                case SIMPL_BOOL_DT:
                    write_SIMPL_BOOL(simpldb, &db->rows.rows[i].datas[j].data.sb);
                    break;
                case SIMPL_DOUBLE_DT:
                    write_SIMPL_DOUBLE(simpldb, &db->rows.rows[i].datas[j].data.sd);
                    break;
                case SIMPL_STR_DT:
                    write_SIMPL_STR(simpldb, &db->rows.rows[i].datas[j].data.ss);
                    break;
                case SIMPL_UUID_DT:
                    write_SIMPL_UUID(simpldb, &db->rows.rows[i].datas[j].data.suuid);
                    break;
                case SIMPL_DATETIME_DT:
                    write_SIMPL_DATETIME(simpldb, &db->rows.rows[i].datas[j].data.sdate);
                    break;
                case SIMPL_BINARY_DATA_DT:
                    write_SIMPL_BINARY_DATA(simpldb, &db->rows.rows[i].datas[j].data.bin);
                    break;
                default:
                    SIMPL_INT tmp;
                    tmp.val = 0;
                    write_SIMPL_INT(simpldb, &tmp);
            }
            

            magic_num[0] = DB_DATA_END;
            WRITE_CURRENT_MAGIC
        }

        magic_num[0] = PAGE_END;
        WRITE_CURRENT_MAGIC
    }

    fclose(simpldb);
    simpldb = NULL;

    free(magic_num);
    magic_num = NULL;
}

void export_simpldb(const char *fname, DATABASE *db) {
    if(fname == NULL || db == NULL) {
        fprintf(stderr, "Please give valid parameters -> export_simpldb.\n");
        return;
    }
    else if(check_file(fname) == true) {
        fprintf(stderr, "This file already exists -> export_simpldb.\n");
        return;
    }

    export_simpldb_data(fname, db);
    export_simpldb_info(fname, db);
}
