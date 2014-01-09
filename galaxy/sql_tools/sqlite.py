# sqlite.py version:0.3
# USAGE: python sqlite.py <output_file> <explain_plan> <header> <str_select> <input_file1> <index1> <header1> <input_file2> <index2> <header2> ..

import sys, csv, sqlite3, time

argvs = sys.argv

if ((len(argvs) % 3) != 2):
    print 'Number of argv is incorrect'
    quit()

num_table = (len(argvs) - 4) / 3
print('Number of Tables: ' + str(num_table) + '\n')

output_file = argvs[1]
explain_plan = argvs[2]
output_header = argvs[3]
str_select  = argvs[4]

conn = sqlite3.connect(':memory:')
conn.row_factory = sqlite3.Row
conn.text_factory = lambda x: unicode(x, "utf-8", "ignore")
cur = conn.cursor()

for i in range(1, num_table + 1): 
    input_file  = argvs[3 * i + 2]
    input_header = argvs[3 * i + 3]
    str_index   = argvs[3 * i + 4]
    
    with open(input_file,'rb') as infile:
        dr = csv.reader(infile, delimiter='\t')
        to_db = []
        row_count = 0
        for row in dr:
            row_count += 1
            values =[]
            col_count = 0
            for col in row:
                col_count += 1
                values.append(col)
            if input_header == '1' and row_count == 1:
                header = values
                print('Header:')
            else:
                to_db.append(values)

    # PREPARE DDL&DML
    str_table1 = 't' + str(i) + '('
    str_table2 = 't' + str(i) + '('
    str_value = 'VALUES ('
    for j in range(col_count):
        if input_header == '1':
            print('  ' + str(j+1) + ' ' + header[j]);
            col_name = header[j]
        else:
            col_name = 'c' + str(j + 1)
        str_table1 = str_table1 + col_name + ' NUMERIC'
        str_table2 = str_table2 + col_name + ' ' 
        str_value = str_value + '?'
        if j != col_count - 1:
            str_table1 = str_table1 + ','
            str_table2 = str_table2 + ','
            str_value = str_value + ','
    str_table1 = str_table1 + ')'
    str_table2 = str_table2 + ')'
    str_value = str_value + ')'
    print('')

    # CREATE TABLE
    str_create = 'CREATE TABLE ' + str_table1 + ';'
    print(str_create)
    time_start = time.time()
    cur.execute(str_create)
    print('Elapsed Time: ' + str(time.time() - time_start) + '\n')

    # LOAD DATA
    str_insert = 'INSERT INTO ' + str_table2  + ' ' + str_value + ';'
    print(str_insert)
    time_start = time.time()
    cur.executemany(str_insert, to_db)
    conn.commit()
    print('Elapsed Time: ' + str(time.time() - time_start) + '\n')

    # CREATE INDEX
    array_idx = str_index.split(',')
    for col_idx in array_idx:
        if col_idx != '':
            time_start = time.time()
            if input_header == '1':
                sql_index = 'CREATE INDEX idx_t' + str(i) +'c' + col_idx + ' on t' + str(i) + '(' + header[int(col_idx) - 1] + ');'
            else:
                sql_index = 'CREATE INDEX idx_t' + str(i) +'c' + col_idx + ' on t' + str(i) + '(c' + col_idx + ');'
            cur.execute(sql_index)
            print(sql_index)
            print('Elapsed Time: ' + str(time.time() - time_start) + '\n')

# SELECT
time_start = time.time()
print(str_select)
cur.execute(str_select)
print('Elapsed Time: ' + str(time.time() - time_start) + '\n')

# OUTPUT
out = open(output_file, 'w')

# HEADER
if output_header == '1':
    col_count = 0
    for col in cur.description:
        col_count += 1
        if col_count != len(cur.description):
            out.write(str(col[0]) + '\t')
        else:
            out.write(str(col[0]) + '\n')

# CONTENT
for row in cur:
    col_count = 0
    for col in row:
        col_count += 1
        if col_count != len(row):
            out.write(str(col) + '\t')
        else:
            out.write(str(col) + '\n')

out.close()

# EXPLAIN PLAN
if explain_plan == '1':
    #conn.executescript('.explain on')
    cur.execute('explain query plan ' + str_select)
    for row in cur:
        col_count = 0
        for col in row:
            col_count += 1
            if col_count != len(row):
                print(str(col) + '\t')
            else:
                print(str(col) + '\n')

cur.close()
conn.close()

