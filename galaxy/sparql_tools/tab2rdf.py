# tab2rdf.py version:0.1
# USAGE: python tab2rdf.py <input_file> <output_file> <output_format> <namespace>
#        <s1_col> <p1_val> <o1_col> <o1_uri/val> <s2_col> <p2_val> <o2_col> <o2_uri/val> ..
# USAGE: python tab2rdf.py <input_file> <output_file> <output_format> multi_namespaces <column1> <namaspace1> <column2> <namespace2> ..

import sys, csv, sqlite3, time

argvs = sys.argv
num_triple = (len(argvs) - 4) / 4
print('Number of Triples for One Column: ' + str(num_triple) + '\n')

input_file = argvs[1]
output_file = argvs[2]
output_format = argvs[3]
ns = argvs[4]

# OUTPUT
out = open(output_file, 'w')

with open(input_file,'rb') as infile:
    dr = csv.reader(infile, delimiter='\t')
    row_count = 0
    for row in dr:
        row_count += 1
        values = []
        col_count = 0
        for col in row:
            col_count += 1
            values.append(col)
        for i in range(0, num_triple): 
            s_val = values[int(argvs[4 * i + 5]) - 1]
            p_val = argvs[4 * i + 6]
            o_val = values[int(argvs[4 * i + 7]) - 1]
            if int(argvs[4 * i + 8]) :
                out.write('<' + ns + s_val + '> <' + ns + p_val + '> "' + o_val + '" .\n')
            else :
                out.write('<' + ns + s_val + '> <' + ns + p_val + '> <' + ns + o_val + '> .\n')

out.close()

