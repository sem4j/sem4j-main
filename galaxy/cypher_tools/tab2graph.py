# tab2rdf.py version:0.1
# USAGE: python tab2rdf.py <option> <input_file> <output_file_n> <output_file_r> <nodes_json> <relations_json>

import sys, csv
#import json

argvs = sys.argv
#print('Number of Arg: ' + str(len(argvs)) + '\n')

option = argvs[1]
input_file = argvs[2]
output_file_n = argvs[3]
output_file_r = argvs[4]
n_json = argvs[5]
r_json = argvs[6]

#print(output_file_n)
#print(output_file_r)

# OUTPUT
out_n = open(output_file_n, 'w')
out_r = open(output_file_r, 'w')

out_n.write('[')
out_r.write('[')

with open(input_file,'rb') as infile:
    if 'h' in option:
        infile.readline()
    dr = csv.reader(infile, delimiter='\t')
    row_count = 0
    sep = '\n'
    for row in dr:
        row_count += 1
        n_json_row = n_json
        r_json_row = r_json
        col_count = 0
        for col in row:
            col_count += 1
            n_json_row = n_json_row.replace('__' + str(col_count) + '__', '"' + col +'"').replace(" ","")
            r_json_row = r_json_row.replace('__' + str(col_count) + '__', '"' + col +'"').replace(" ","")
        #print(n_json_row)
        #print(r_json_row)
        out_n.write(sep + n_json_row)
        out_r.write(sep + r_json_row)
        sep = ',\n'

out_n.write('\n]')
out_r.write('\n]')

out_n.close()
out_r.close()
