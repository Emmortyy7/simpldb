
// This helps make code look better and more organised.

/*

get_index_by_pos_location -> return 9 check that function works properly or not
change file read limit
in csv_read (or readline i dont remember) you might have to zero out the buffer before reading each line ask this to gpt.
adding extra because of end of the file may cause error -> get_csv_info
check for closing file in the main funcs
readline zero out the buffer before reading it maybe and csv_read too its important 
create_string_by_row if statements -> bin if statements ERROR!!
const char params
calc_row there is a switch case and i calculated sizeof of DB_RAW_DATA but probably i should do that spereately for each data !!!
when writing uuid_t there is must be data size but uuid_t was char* but maybe im wrong

*/

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
