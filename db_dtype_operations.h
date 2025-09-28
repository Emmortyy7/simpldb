#include "main_lib.h"
#include "db_custom_data_types.h"

// Example of headers. Predefined for general use. 
#define JPEG_HEADER_MAGIC_NUMBER 0x4A50 // JP
#define PNG_HEADER_MAGIC_NUMBER  0x504E // PN
#define TXT_HEADER_MAGIC_NUMBER  0x5458 // TX
// Giving it a header is optional.
#define NO_HEADER                0x4E4F // NO

#define DATETIME_BUF_LENGTH 12 // YY:MM:DD:HH:MN:SS which is total 12 bytes (with no ':' ofc)
#define UUID_LENGTH 36 // UUID's are 36 char length so i put this thing in here for malloc

#pragma once

// core data types to_string funcs:

void simpl_int_to_str(SIMPL_INT *si, char *ptr) {
    if(si == NULL) {
        fprintf(stderr, "Please give valid data -> simpl_int_to_str.\n");
        return;
    }
    sprintf(ptr, "%lld", (unsigned long long)si->val);
}

void simpl_float_to_str(SIMPL_FLOAT *sf, char *ptr) {
    if(sf == NULL) {
        fprintf(stderr, "Please give valid data -> simpl_float_to_str.\n");
        return;
    }
  
    sprintf(ptr, "%f", sf->val);
}

void simpl_double_to_str(SIMPL_DOUBLE *sd, char *ptr) {
    if(sd == NULL) {
        fprintf(stderr, "Please give valid data -> simpl_double_to_str.\n");
        return;
    }
    sprintf(ptr, "%lf", sd->val);
}

void simpl_bool_to_str(SIMPL_BOOL *sb, char *ptr) {
    if(sb == NULL) {
        fprintf(stderr, "Please give valid data -> simpl_bool_to_str.\n");
        return;
    }

    if(sb->val == true) {
        sprintf(ptr, "True");
    }
    else {
        sprintf(ptr, "False");
    }
}

// UUID funcs:

char* get_uuid_str(SIMPL_UUID *uuid) {
    if(uuid == NULL) {
        fprintf(stderr, "Please give valid data -> get_uuid_str.\n");
        return NULL;
    }
    fprintf(stdout, "I don't recommend using this func because string version of uuid is already in your SIMPLE_UUID struct as uuid_str.val");
    return  uuid->uuid_str.val;
}

// uuid_t is basically a pointer so when giving it as an argument no need for using &.
void generate_uuid(SIMPL_UUID *suuid) { 
    if(suuid == NULL) {
        fprintf(stderr, "Please give valid data -> generate_uuid.\n");
        return;
    }
   
    uuid_generate(suuid->raw_uuid);
    if(suuid->uuid_str.val == NULL)
        suuid->uuid_str.val = (char*)malloc(UUID_LENGTH + 1);
    uuid_unparse(suuid->raw_uuid, suuid->uuid_str.val);
}


// Datetime funcs:

bool isDateTimeValid(SIMPL_DATETIME *sdt, SIMPL_DATETIME_INFO *info) {
    if(sdt == NULL) {
        fprintf(stderr, "Please give valid data -> isDateTimeValid.\n");
        return false;
    }
    
    if(sdt->day.val < 0 || sdt->hour.val < 0 || sdt->minute.val < 0 || sdt->second.val < 0 || sdt->month.val < 0 || sdt->year.val < 0) {
        return false;
    }
    else if((sdt->month.val == 1 || sdt->month.val == 3 || sdt->month.val == 5 || sdt->month.val == 7 || sdt->month.val == 8 || sdt->month.val == 10 || sdt->month.val == 12) && sdt->day.val > 31) {
        return false;
    }
    else if((sdt->month.val == 4 || sdt->month.val == 6 || sdt->month.val == 9 || sdt->month.val == 11) && sdt->day.val > 30) {
        return false;
    }
    else if(sdt->month.val == 2) {
        if((sdt->year.val % 4 == 0) && sdt->day.val > 29) {
            return false;
        }
        else if(sdt->day.val > 28) {
            return false;
        }
    }
    
    if(info == NULL) {
        if(sdt->year.val < 1940 || sdt->year.val > 2025) {
            return false;
        }
        else if(sdt->hour.val > 24 || sdt->minute.val > 60 || sdt->second.val > 60) {
            return false;
        }
    }
    else {
        if(sdt->year.val < info->min_year.val || sdt->year.val > info->max_year.val) {
            return false;
        }
        if(info->am_pm == true) {
            if(sdt->hour.val > 12 || sdt->minute.val > 60 || sdt->second.val > 60) {
                return false;
            }
        }
        else {
            if(sdt->hour.val > 24 || sdt->minute.val > 60 || sdt->second.val > 60) {
                return false;
            }
        }
    }

    return true;
}

static char* datetime_two_byte_int_to_str(uint64_t val) {
    char *buf = (char*)malloc(3*sizeof(char));
    
    if(buf == NULL) {
        return NULL;
    }

    int first = val / 10;
    int sec = val % 10;
    
    sprintf(buf, "%d%d", first, sec);

    return buf;
}

void simpl_datetime_to_str(SIMPL_DATETIME *sdt, char *ptr) {
    if(sdt == NULL) {
        fprintf(stderr, "Please give valid data -> simpl_datetime_to_str.\n");
        return;
    }
    
    char *datetime = (char*)malloc(DATETIME_BUF_LENGTH*sizeof(char));
    
    char *year_ptr = datetime_two_byte_int_to_str(sdt->year.val);
    char *month_ptr = datetime_two_byte_int_to_str(sdt->month.val);
    char *day_ptr = datetime_two_byte_int_to_str(sdt->day.val);
    char *hour_ptr = datetime_two_byte_int_to_str(sdt->hour.val);
    char *min_ptr = datetime_two_byte_int_to_str(sdt->minute.val);
    char *second_ptr = datetime_two_byte_int_to_str(sdt->second.val);

    if(year_ptr == NULL || month_ptr == NULL || day_ptr == NULL || hour_ptr == NULL || min_ptr == NULL || second_ptr == NULL) {
        if(year_ptr != NULL)
            free(year_ptr);
        if(month_ptr != NULL)
            free(month_ptr);
        if(day_ptr != NULL)
            free(day_ptr);
        if(hour_ptr != NULL)
            free(hour_ptr);
        if(min_ptr != NULL)
            free(min_ptr);
        if(second_ptr != NULL)
            free(second_ptr);
        if(datetime != NULL)
            free(datetime);
        return;
    }

    if(sdt->year.val < 10) {
        datetime[0] = '0';
        datetime[1] = year_ptr[1];
    }
    else {
        datetime[0] = year_ptr[0];
        datetime[1] = year_ptr[1];
    }

    if(sdt->month.val < 10) {
        datetime[2] = '0';
        datetime[3] = month_ptr[1];
    }
    else {
        datetime[2] = month_ptr[0];
        datetime[3] = month_ptr[1];
    }

    if(sdt->day.val < 10) {
        datetime[4] = '0';
        datetime[5] = day_ptr[1];
    }
    else {
        datetime[4] = day_ptr[0];
        datetime[5] = day_ptr[1];
    }

    if(sdt->hour.val < 10) {
        datetime[6] = '0';
        datetime[7] = hour_ptr[1];
    }
    else {
        datetime[6] = hour_ptr[0];
        datetime[7] = hour_ptr[1];
    }

    if(sdt->minute.val < 10) {
        datetime[8] = '0';
        datetime[9] = min_ptr[1];
    }
    else {
        datetime[8] = min_ptr[0];
        datetime[9] = min_ptr[1];
    }

    if(sdt->second.val < 10) {
        datetime[10] = '0';
        datetime[11] = second_ptr[1];
    }
    else {
        datetime[10] = second_ptr[0];
        datetime[11] = second_ptr[1];
    }

    strcpy(ptr, datetime);

    if(year_ptr != NULL)
        free(year_ptr);
    if(month_ptr != NULL)
        free(month_ptr);
    if(day_ptr != NULL)
        free(day_ptr);
    if(hour_ptr != NULL)
        free(hour_ptr);
    if(min_ptr != NULL)
        free(min_ptr);
    if(second_ptr != NULL)
        free(second_ptr);
    if(datetime != NULL)
        free(datetime);
}

// Can read any type of file because it reads their binary.
void read_files(char *fpath, SIMPL_BINARY_DATA *bin, bool low_mem, uint16_t file_type_header) {
    if(fpath == NULL || bin == NULL) {
        fprintf(stderr, "Please give valid parameters -> read_files.\n");
        return;
    }
    
    FILE *file = fopen(fpath, "rb");
    
    if(file == NULL) {
        fprintf(stderr, "File can't be opened -> read_files.\n");
        return;
    }
        
    fseek(file, 0, SEEK_END);
    bin->length_by_bytes = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    if(bin->length_by_bytes > 0xFFFF || low_mem == true) { 
        fprintf(stderr, "low memory mode limit exceeded file is bigger than 0xFFFF -> read_files.\n");
        bin->length_by_bytes = 0;
        fclose(file);
        return;
    }

    bin->data = (uint8_t*)malloc(bin->length_by_bytes);

    if(bin->data == NULL) {
        fprintf(stderr, "Failed to allocate memory can't read the file -> read_files.\n");
        bin->length_by_bytes = 0;
        fclose(file);
        return;
    }

    fread(bin->data, 1, bin->length_by_bytes, file);
    fclose(file);

    bin->header = file_type_header;
}
