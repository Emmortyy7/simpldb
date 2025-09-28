#pragma once

#include "main_lib.h"
#include "db_custom_data_types.h"
#include "db_dtype_operations.h"

#define LINE_BUF 2048 // Just a buffer macro

#define SET_START 1 // Macro for a helper func. It's used for making file pointer start from beginning of file

#define CSV_HIGH_MEM_HIGH_PERF 1 // This makes reading csv file faster but so much memory use
#define CSV_LOW_MEM_LOW_PERF 0 // Slow but most secure and low-memory file reading

/*
   
    This code contains structure of database.
    DB_RAW_DATA is the smallest part of the simpl db and its just a data that is part of the row.
    DB_DATA is DB_RAW_DATA with dtype info.
    ROW is an array of datas with count of the data and index info which says its location with different ROW datas.
    COLUMN is is just a column that has its special name given by user and its index which shows the location of COLUMN data.
    ROWS and COLUMNS just are arrays that holds ROW datas and COLUMN datas with their count variables that says how many ROW, COLUMN they have.
    DATABASE is combination of ROWS and COLUMNS.

    !!! : Only ; and , accepted as csv delimeters.
 
 */

typedef union DB_RAW_DATA {
    SIMPL_INT si;
    SIMPL_FLOAT sf;
    SIMPL_DOUBLE sd;
    SIMPL_STR ss;
    SIMPL_BOOL sb;
    SIMPL_DATETIME sdate;
    SIMPL_UUID suuid;
    SIMPL_BINARY_DATA bin;
} DB_RAW_DATA;

typedef struct DB_DATA {
    SIMPL_DT_LIST dtype;
    DB_RAW_DATA data;
    uint16_t pos;
} DB_DATA;

typedef struct ROW {
    DB_DATA *datas;
    uint16_t number_of_datas; 
    uint16_t pos;
} ROW;

typedef struct COLUMN { 
    SIMPL_STR col_name;
    uint16_t col_index;
} COLUMN;

typedef struct ROWS {
    ROW* rows;
    uint16_t row_count;
} ROWS;

typedef struct COLUMNS {
    COLUMN *columns;
    uint16_t col_count;
} COLUMNS;

typedef struct DATABASE {
    ROWS rows;
    COLUMNS columns;
} DATABASE;

// CSV:

typedef struct CSV_INFO {
    char delimeter;
    uint16_t line_count;
} CSV_INFO;

void finish_cleanup(DATABASE **db) {
    free(*db);
    *db = NULL;
}

// Cleans up the db.
void kill_db(DATABASE *db) {
    if(db == NULL) {
        fprintf(stderr, "Given database is NULL.\n");
        return;
    }
    for(uint16_t i = 0; i < db->rows.row_count; i++) {
        if(db->rows.rows[i].datas != NULL) {
            free(db->rows.rows[i].datas);
            db->rows.rows[i].datas = NULL;       
        }
    }
    if(db->rows.rows != NULL) {
        free(db->rows.rows);
        db->rows.rows = NULL;    
    }
    for(uint16_t i = 0; i < db->columns.col_count; i++) {
        if(db->columns.columns[i].col_name.val != NULL) {
            free(db->columns.columns[i].col_name.val);
            db->columns.columns[i].col_name.val = NULL;       
        }
    }
    if(db->columns.columns != NULL) {
        free(db->columns.columns);
        db->columns.columns = NULL;
    }
}

// Builds the db.
DATABASE* create_db() {
    DATABASE* db = (DATABASE*)malloc(sizeof(DATABASE));
    if(db == NULL) {
        fprintf(stderr, "Can't allocate database -> create_db.\n");
        return NULL;
    }

    memset(db, 0, sizeof(DATABASE));
    
    return db;
}

// Reshapes the each row.Forexample you have 3 elements in your array but you have 6 columns it allocates 3 more to fit the datas.
bool reshape_rows_in_db(DATABASE *db, uint16_t col_c) {
    if(db == NULL) {
        fprintf(stderr, "Database is NULL -> reshape_rows_in_db.\n");
        return false;
    }
    for(uint16_t i = 0; i < db->rows.row_count; i++) {
        uint16_t old_col_c = db->rows.rows[i].number_of_datas;
        
        DB_DATA *datas = (DB_DATA*)realloc(db->rows.rows[i].datas, col_c*sizeof(DB_DATA));
        if(datas == NULL) {return false;}

        for(uint16_t j = old_col_c; j < col_c; ++j) {memset(&datas[j], 0, sizeof(DB_DATA));}
        
        db->rows.rows[i].datas = datas;
        db->rows.rows[i].number_of_datas = col_c;
    }
    return true;
}

// Adds column and reshapes each row for those new columns.
void add_column(DATABASE *db, SIMPL_STR *names, bool file_read_mode) {
    if(db == NULL) {
        fprintf(stderr, "Database is NULL -> add_column.\n");
        return;
    }
    else if(names == NULL) {
        fprintf(stderr, "Please give some valid names -> add_column.\n");
        return;
    }

    uint16_t name_count;
    for(name_count = 0; names[name_count].val != NULL; name_count++);
    
    COLUMNS *new_columns = (COLUMNS*)malloc(sizeof(COLUMNS));
    new_columns->columns = (COLUMN*)malloc(sizeof(COLUMN)*(name_count + db->columns.col_count));
    
    if(!file_read_mode) {new_columns->col_count = db->columns.col_count + name_count;}

    for(uint16_t i = 0; i < db->columns.col_count; i++) {
        new_columns->columns[i].col_index = i;
        new_columns->columns[i].col_name.val = (char*)malloc(strlen(db->columns.columns[i].col_name.val)*sizeof(char) + 1);
        strcpy(new_columns->columns[i].col_name.val, db->columns.columns[i].col_name.val);
    }
    for(uint16_t i = 0; i < name_count; i++) {
        new_columns->columns[db->columns.col_count + i].col_index = db->columns.col_count + i;
        new_columns->columns[db->columns.col_count + i].col_name.val = (char*)malloc(strlen(names[i].val)*sizeof(char) + 1); 
        strcpy(new_columns->columns[db->columns.col_count + i].col_name.val, names[i].val);
    }

    for(uint16_t i = 0; i < db->columns.col_count; i++) {
        if(db->columns.columns[i].col_name.val != NULL) {
            free(db->columns.columns[i].col_name.val);
            db->columns.columns[i].col_name.val = NULL;       
        }
    }
    if(db->columns.columns != NULL) {
        free(db->columns.columns);
        db->columns.columns = NULL;
    }

    db->columns.columns = new_columns->columns;
    db->columns.col_count = new_columns->col_count;
    
    // Most use cases will be like this but user can add column after making some rows.
    if(db->rows.row_count == 0 && db->rows.rows == NULL) {
        return;
    }
    else {
        bool reshaping_status = reshape_rows_in_db(db, db->columns.col_count);
        if(reshaping_status == false) {
            fprintf(stderr, "Allocation error.\nadd_column->reshape_rows_in_db could not reallocate rows.\nKilling db -> kill_db.\n");
            kill_db(db);
        }
    }
    return;
}

// Turns normal data into usable DB_DATA object.
DB_DATA* prep_single_data(void *data, SIMPL_DT_LIST dtype, uint16_t *specific_pos) {
    if(data == NULL) {
        fprintf(stderr, "Please give valid data -> prep_single_data.\n");
        return NULL;
    }
      
    DB_DATA *new_data = (DB_DATA*)malloc(sizeof(DB_DATA));

    if(new_data == NULL) {
        fprintf(stderr, "Can't allocate DB_DATA -> prep_data\n");
        return NULL;
    }
    
    new_data->dtype = dtype;
    
    if(specific_pos != NULL) {
        new_data->pos = *(specific_pos) + 10; // i add 10 to each one becasue default int 0 and if there is no specific pos it will be set to 0 which can cause logical errors.
    }

    if(dtype == SIMPL_BINARY_DATA_DT) {
        new_data->data.bin = *((SIMPL_BINARY_DATA*)data);
    }
    else if(dtype == SIMPL_DATETIME_DT) {
        new_data->data.sdate = *((SIMPL_DATETIME*)data);
    }
    else if(dtype == SIMPL_UUID_DT) {
        new_data->data.suuid = *((SIMPL_UUID*)data);
    }
    else if(dtype == SIMPL_STR_DT) {
        new_data->data.ss = *((SIMPL_STR*)data);
    }
    else if(dtype == SIMPL_BOOL_DT) {
        new_data->data.sb = *((SIMPL_BOOL*)data);
    }
    else if(dtype == SIMPL_INT_DT) {
        new_data->data.si = *((SIMPL_INT*)data);
    }
    else if(dtype == SIMPL_FLOAT_DT) {
        new_data->data.sf = *((SIMPL_FLOAT*)data);
    }
    else if(dtype == SIMPL_DOUBLE_DT) {
        new_data->data.sd = *((SIMPL_DOUBLE*)data);
    }

    return new_data;
}

// Creates the new row in memory.
void create_row(DATABASE *db, uint8_t file_read_mode) {
    if(db == NULL) {
        fprintf(stderr, "Database is NULL -> create_row.\n");
        return;
    }
    
    if(db->rows.rows == NULL && db->rows.row_count == 0) {
        db->rows.rows = (ROW*)malloc(sizeof(ROW));
        db->rows.row_count = 1;
        return;
    }
    // for handling row creation in add_row. if value is not 255 there will be no allocation we need this because we pre allocate the whole row while reading file.
    else if(file_read_mode > 0) {
        if(file_read_mode == 255) {
            db->rows.rows = (ROW*)malloc(db->rows.row_count*sizeof(ROW));
        }
        return;
    }
    else {
        ROW* temp_r_ptr = (ROW*)realloc(db->rows.rows, (db->rows.row_count + 1) * sizeof(ROW));
        if(temp_r_ptr == NULL) {
            fprintf(stderr, "Can't create a row killing db.\ncreate_row -> kill_db.\n");
            kill_db(db);
            return;
        }
        db->rows.rows = temp_r_ptr;
    }

    db->rows.row_count++;
}

// Returns the index of the data by looking its pos variable.
uint16_t get_index_by_pos_location(uint16_t index, uint16_t col_c, DB_DATA *datas) {
    for(uint16_t i = 0; i < col_c; i++) {
        if(datas[i].pos == index) {
            return i;
        }
    }
    fprintf(stderr, "get_index_by_pos_location -> didn't work properly.\n");
    return 9; // it cant be under 10 so returning 9 will indicate that function does not work properly
}

// Adds the row to the db.
void add_row(DATABASE *db, DB_DATA *datas) {
    if(db == NULL) {
        fprintf(stderr, "Database is NULL -> add_row.\n");
        return;
    }
    else if(datas == NULL) {
        fprintf(stderr, "Please give valid datas -> add_row.\n");
        return;
    }
    
    if(db->columns.columns == NULL || db->columns.col_count == 0) {
        fprintf(stderr, "You need column/columns to add row -> add_row.\n");
        return;
    }

    uint16_t specific_index_value_indexes[db->columns.col_count];
    uint16_t tmp = 0;
    for(uint16_t i = 0; i < db->columns.col_count; i++) {
        if(datas[i].pos >= 10) {
            specific_index_value_indexes[tmp] = datas[i].pos;
            tmp += 1;
        }
    }
    for(uint16_t i = 0; i < db->columns.col_count; i++) {
        for(uint16_t j = i + 1; j < db->columns.col_count; j++) {
            if(specific_index_value_indexes[i] == specific_index_value_indexes[j]) {
                fprintf(stderr, "Can't have 2 elements in same location -> add_row.\n");
                return;
            }
        }
    }
    
    create_row(db, 0);
    db->rows.rows[db->rows.row_count - 1].number_of_datas = db->columns.col_count;
    db->rows.rows[db->rows.row_count - 1].datas = (DB_DATA*)malloc(db->columns.col_count*sizeof(DB_DATA));
    
    for(uint16_t i = 0; i < tmp; i++) {
        db->rows.rows[db->rows.row_count - 1].datas[specific_index_value_indexes[i] - 10] = datas[get_index_by_pos_location(specific_index_value_indexes[i], db->columns.col_count, datas)];
    }

    for(uint16_t i = 0; i < db->columns.col_count; i++) {
        if(db->rows.rows[db->rows.row_count - 1].datas[i].pos >= 10) {
            continue;
        }
        else {
            db->rows.rows[db->rows.row_count - 1].datas[i] = datas[i];
        }
    }

    return;
}

// CSV OPERATIONS:

// Reads line. seek_set_bool is used for iterating form the start or at the current line pass NULL if you dont want to use that. 
char* readline(FILE *fptr, uint16_t wanted_line, uint16_t cur_line, uint8_t *seek_set_bool) {
    if(fptr == NULL) {
        fprintf(stderr, "Can't proceed with empty file -> readline.\n");
        return NULL;
    }

    bool status = true;
    
    if(wanted_line < cur_line || seek_set_bool != NULL) {
        fseek(fptr, 0, SEEK_SET);
    }
    
    char *line = (char*)malloc(LINE_BUF*sizeof(char));
    memset(line, 0, LINE_BUF);
    
    do{
        fgets(line, LINE_BUF, fptr);
        
        if(feof(fptr)) {
            return NULL;
        }
        else if(cur_line == wanted_line) {
            status = false;
        }
        cur_line++;
    }while(status == true);

    return line;
}

// Counts lines of a file for using loops safely 
uint16_t count_line(FILE *fptr, uint8_t MEM_MODE, uint32_t BUF_SIZ) {
    uint16_t line_count = 0;

    if(MEM_MODE == CSV_LOW_MEM_LOW_PERF) {
        int ch;
        while((ch = fgetc(fptr)) != EOF) {
            if(ch == '\n') {line_count++;}
        }
        
        if(line_count > 0 && ch != '\n') {line_count++;} // this is for if file does not end with a new line it adds it as a new line because file is finished. !!! : can cause ERRORS:W
        return line_count;
    }
    else if(MEM_MODE == CSV_HIGH_MEM_HIGH_PERF) {
        char buf[BUF_SIZ];
        uint32_t bytes;

        while((bytes = fread(buf, 1, BUF_SIZ, fptr))) {
            for(uint32_t i = 0; i < bytes; i++) {
                if(buf[i] == '\n') {
                    line_count++;
                }
            }
        }

        if(bytes > 0 && buf[bytes - 1] != '\n') {
            line_count++;
        }
        return line_count;
    }
    return line_count; 
}

// Gets the needed data for reading csv
struct CSV_INFO* get_csv_info(char *fpath, uint8_t MEM_MODE, uint32_t BUFFER_FOR_HIGH_MEM_MODE) {
    if(fpath == NULL) {
        fprintf(stderr, "Please give valid fpath. -> get_csv_info\n");
        return NULL;
    }

    struct CSV_INFO *csv_i = (struct CSV_INFO*)malloc(sizeof(CSV_INFO)); 

    uint8_t seek_set = SET_START;

    FILE *fptr = fopen(fpath, "r");
    char *lptr = readline(fptr, 1, 0, &seek_set);
    
    char delim;
    uint16_t line_count;

    for(uint16_t i = 0; *(lptr + i) != '\0'; i++) {
        if(*(lptr + i) == ',') {delim = ',';}
        else if(*(lptr + i) == ';') {delim = ';';}
    }

    if(!(delim == ',' || delim == ';')) {
        fprintf(stderr, "Csv file must have valid delimeters for this db (, or ;). -> get_csv_info\n");
        return NULL;
    }
    
    line_count = count_line(fptr, MEM_MODE, BUFFER_FOR_HIGH_MEM_MODE);
    
    csv_i->line_count = line_count;
    csv_i->delimeter = delim;

    free(lptr);
    fclose(fptr);

    return csv_i;
}

// Parses each word from the line knowing the delimeter thanks to delim param.
SIMPL_STR* parse_line(char *line, char delim, uint16_t *col_count) {
    SIMPL_STR *main = (SIMPL_STR*)malloc(sizeof(SIMPL_STR)*32);
    uint16_t main_word_counter = 0;
    uint16_t main_char_counter = 0;

    for(uint16_t i = 0; i < 32; i++) {
       main[i].val = (char*)malloc(sizeof(char)*128);
    }

    for(uint16_t i = 0; i < 256 && *(line + i) != '\0'; i++) {
        if(main_word_counter >= 32) {
            break;
        }
        else if(*(line + i) == delim) {
            main[main_word_counter].val[main_char_counter] = '\0';
            main_char_counter = 0;
            main_word_counter++;
            continue;
        }
        else {
            main[main_word_counter].val[main_char_counter] = *(line + i);
            main_char_counter++;
        }
    }

    main[main_word_counter].val[main_char_counter] = '\0';  
    
    if(col_count != NULL) {*col_count = main_word_counter + (main_char_counter > 0);}

    return main;
}

// Turns parsed strings into DB_DATA array.
DB_DATA* prep_csv_line_data(SIMPL_STR *names, uint16_t col_c) {
    DB_DATA *datas = (DB_DATA*)malloc(col_c*sizeof(DB_DATA));

    for(uint16_t i = 0; i < col_c; i++) {
        datas[i].dtype = SIMPL_STR_DT;
        datas[i].data.ss = names[i];
    }

    return datas;
}

// Reads the db and exports it into csv.
struct CSV_INFO* read_csv(DATABASE *db, char *fpath, uint8_t MEM_MODE, uint32_t BUFFER_FOR_HIGH_MEM_MODE, bool NULL_OUTPUT) {
    if(db == NULL || fpath == NULL || !(MEM_MODE == CSV_LOW_MEM_LOW_PERF || MEM_MODE == CSV_HIGH_MEM_HIGH_PERF)) {
        fprintf(stderr, "Please give valid parameters -> read_csv.\n");
        return NULL;
    }
    
    FILE *fptr = fopen(fpath, "r");
    if(fptr == NULL) {
        fprintf(stderr, "Fpath is not valid or can't open file -> read_csv.\n");
    }

    struct CSV_INFO *csv_i = get_csv_info(fpath, MEM_MODE, BUFFER_FOR_HIGH_MEM_MODE);
    uint8_t seek_set_bool = SET_START;

    char *line = readline(fptr, 1, 0, &seek_set_bool);
    uint16_t col_c;
    SIMPL_STR *names = parse_line(line, csv_i->delimeter, &col_c);
    
    if(col_c <= 0) {
        free(csv_i);
        free(line);
        free(names);
        fclose(fptr);
        fprintf(stderr, "Check the CSV file, it's weird can't read. -> read_csv.\n");
        return NULL;
    }
    
    uint16_t old_col_count = db->columns.col_count;
    db->columns.col_count = db->columns.col_count + col_c;
    add_column(db, names, false);
    
    free(line);
    for(uint16_t i = 0; i < col_c; i++) {
        free(names[i].val);
    }
    free(names);
    names = NULL;
    line = NULL;
    
    fseek(fptr, 0, SEEK_SET);
    char delim = csv_i->delimeter;

    for(uint16_t i = 2; i < csv_i->line_count; i++) {
        line = readline(fptr, i, 1, NULL);
        names = parse_line(line, delim, NULL);
        
        DB_DATA *datas = prep_csv_line_data(names, col_c);
        for(uint16_t i = 0; i < col_c; i++) {
            datas[i].pos = old_col_count + 1 + i;
        }

        add_row(db, datas);

        free(line);
        for(uint16_t i = 0; i < col_c; i++) {
            free(names[i].val);
        }
        names = NULL;
        line = NULL;
    }

    if(NULL_OUTPUT == false) {
        fclose(fptr);
        return csv_i;
    }
    free(csv_i);
    fclose(fptr);
    return NULL;
}

// This is an insecure func that just works securely in my code so made it static. This is for appending two strings.
static void strapp(char* main, char* app, uint16_t BUFSIZE) {
    if(BUFSIZE <= 0 || main == NULL || app == NULL) {
        fprintf(stderr, "Wrong params -> strapp.\n");
        return;
    }
    
    bool START = false;
    uint16_t app_c = 0;
    for(uint16_t i = 0; i < BUFSIZE; i++) {
        if(START == false && *(main + i) == '\0') {
            START = true;
            main[i] = *(app + app_c);
            app_c++;
        }
        else {
            main[i] = *(app + app_c);
            app_c++;
        }
    }
}

// Turns row or columns (mainly rows so naming convention doesn't have cols) into string (file line for csv).
char* create_string_by_row(ROW* row, COLUMNS *cols, char delim) {
    char* str = (char*)malloc(sizeof(char)*LINE_BUF);
    char delim_str[2];
    delim_str[0] = delim;
    delim_str[1] = '\0';

    if(str == NULL) {
        fprintf(stderr, "Can't allocate memory -> create_string_by_row.\n");
        return NULL;
    }
    else if((cols == NULL && row == NULL) || (cols !=NULL && row != NULL)) {
        fprintf(stderr, "Wrong parameters probably you killed the whole csv file doing this -> create_string_by_row.\n");
        return NULL;
    }
    
    if(cols != NULL) {
        for(uint16_t i = 0; i < cols->col_count; i++) {
            strapp(str, cols->columns[i].col_name.val, LINE_BUF);
        }
    }
    else {
        for(uint16_t i = 0; i < row->number_of_datas; i++) {
            if(row->datas[i].dtype == SIMPL_STR_DT) {
                strapp(str, row->datas[i].data.ss.val, LINE_BUF);
            }
            // MAY CAUSE ERRORS DEBUG!!!!!
            else if(row->datas[i].dtype == SIMPL_BINARY_DATA_DT) {
                if(row->datas[i].data.bin.header != NO_HEADER && row->datas[i].data.bin.header != 0) {
                    strapp(str, (char*)row->datas[i].data.bin.header, LINE_BUF);
                }
                else {
                    char *tmp = (char*)malloc(sizeof(char)*13);
                    strcpy(tmp, "NO_HEADER_BIN");
                    strapp(str,tmp, LINE_BUF);
                    free(tmp);
                }
            }
            else if(row->datas[i].dtype == SIMPL_UUID_DT) {
                strapp(str, row->datas[i].data.suuid.uuid_str.val, LINE_BUF);
            }
            else if(row->datas[i].dtype == SIMPL_BOOL_DT) { // Booleans will be written like "True" and "False" for simplier python usage of csv file. 
                if(row->datas[i].data.sb.val == true) {
                    char *tmp = (char*)malloc(sizeof(char)*4);
                    strcpy(tmp, "True");
                    strapp(str, tmp, LINE_BUF);
                    free(tmp);
                }
                else {
                    char *tmp = (char*)malloc(sizeof(char)*5);
                    strcpy(tmp, "False");
                    strapp(str, tmp, LINE_BUF);
                    free(tmp);
                }
            }
            else if(row->datas[i].dtype == SIMPL_INT_DT) {
                char *tmp = (char*)malloc(sizeof(char)*256);
                simpl_int_to_str(&(row->datas[i].data.si), tmp);
                strapp(str, tmp, LINE_BUF);
                free(tmp);
            }
            else if(row->datas[i].dtype == SIMPL_FLOAT_DT) {
                char *tmp = (char*)malloc(sizeof(char)*256);
                simpl_float_to_str(&(row->datas[i].data.sf), tmp);
                strapp(str, tmp, LINE_BUF);
                free(tmp);
            }
            else if(row->datas[i].dtype == SIMPL_DOUBLE_DT) {
                char *tmp = (char*)malloc(sizeof(char)*256);
                simpl_double_to_str(&(row->datas[i].data.sd), tmp);
                strapp(str, tmp, LINE_BUF);
                free(tmp);
            }
            else if(row->datas[i].dtype == SIMPL_DATETIME_DT) {
                char *tmp = (char*)malloc(sizeof(char)*DATETIME_BUF_LENGTH);
                simpl_datetime_to_str(&(row->datas[i].data.sdate), tmp);
                char ch = ':';
                
                // This is for 12:12:12:12:12 thing.
                for(uint8_t i = 0; i < DATETIME_BUF_LENGTH; i++) {
                    if(i != 0 && ((i % 2) == 0)) {
                        strapp(str, &ch, LINE_BUF);
                    }
                    strapp(str, &tmp[i], LINE_BUF);
                }

                free(tmp);
            }
            else {
                char *tmp = (char*)malloc(sizeof(char)*3);
                strcpy(tmp, "NaN");
                strapp(str, tmp, LINE_BUF);
                free(tmp);
            }
            strapp(str, delim_str, LINE_BUF);
        }
    }
    
    uint16_t real_size;
    for(real_size = 0; *(str + real_size) != '\0'; real_size++);
    char *new_ptr = (char*)realloc(str, real_size + 1);
    
    if(!new_ptr && real_size != 0) {
        free(str);
        return NULL;
    }

    return new_ptr;
}

void export_as_csv(char *fname, DATABASE *db, char delim) {
    if(delim != ',' && delim != ';') {
        fprintf(stderr, "Please choose supported delimeter (, or ;) -> export_as_csv.\n");
        return;
    }
    else if(db == NULL) {
        fprintf(stderr, "Please give a vaild DATABASE -> export_as_csv.\n");
        return;
    }

    FILE *fptr = fopen(fname, "w");
    if(fptr == NULL) {
        fprintf(stderr, "Can't open or create the csv file -> export_as_csv.\n");
        return;
    }

    for(uint16_t i = 0; i < db->rows.row_count; i++) {
        if(i == 0) {
            char *ptr = create_string_by_row(NULL, &db->columns, delim); 
            fprintf(fptr, "%s\n", ptr);
            free(ptr);
            continue;
        }
        else {
            char *ptr = create_string_by_row(&db->rows.rows[i], NULL, delim);
            fprintf(fptr, "%s\n", ptr);
            free(ptr);
            continue;
        }
    }

    fclose(fptr);
}
