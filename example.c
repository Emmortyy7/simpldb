#include <stdio.h> 
#include <stdlib.h>
#include "db_custom_data_types.h"
#include "simpldb_core.h"
#include "simpldb_custom_file_type_io.h"

#define COLUMN_COUNT 4
#define COL_NAME_LENGTH 8
#define ROW_COUNT 3
#define DATA_COUNT_FOR_EACH_ROW COLUMN_COUNT

typedef struct ROW_DATAS {
  DB_DATA *each_row;
} ROW_DATAS;

int main(int argc, char *argv[]) {
  DATABASE *db;
  SIMPL_STR *col_names = malloc(COLUMN_COUNT*sizeof(SIMPL_STR));
  ROW_DATAS *row_datas = malloc(ROW_COUNT*sizeof(ROW_DATAS));

  db = create_db();

  for(uint8_t i = 0; i < COLUMN_COUNT; i++) {
    col_names[i].val = malloc(COL_NAME_LENGTH*sizeof(char));
  }
  for(uint8_t i = 0; i < ROW_COUNT; i++) {
    row_datas[i].each_row = malloc(DATA_COUNT_FOR_EACH_ROW*sizeof(DB_DATA));
  }  

  // preparing columns.  
  col_names[0].val = strcpy(col_names[0].val, "test1");
  col_names[1].val = strcpy(col_names[1].val, "test2");
  col_names[2].val = strcpy(col_names[2].val, "test3");
  col_names[3].val = strcpy(col_names[3].val, "test4");

  // preparing rows.
  // Im gonna write a garbage data.
  
  char *tmp_str = malloc(3*sizeof(char)); // random garbage data for testing.
  strcpy(tmp_str, "abc");
  
  for(uint8_t i = 0; i < ROW_COUNT; i++) {
    for(uint8_t j = 0; j < DATA_COUNT_FOR_EACH_ROW; j++) {
      row_datas[i].each_row[j].dtype = SIMPL_STR_DT;
      strcpy(row_datas[i].each_row[j].data.ss.val, tmp_str);
      
      // testing specific pos feature.
      if(j == 0) {
        row_datas[j].each_row[j].pos = 4;
      }
    }  
  }

  // adding columns and rows.
  add_column(db, col_names, false);
  add_row(db, row_datas->each_row);

  // exporting as csv and .simpldb
  export_simpldb("test", db);
  export_as_csv("test", db, ',');
}
