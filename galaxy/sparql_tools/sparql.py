# sparql.py version:0.1
# USAGE: python sparql.py <str_select> <input_file> <output_file>

import sys, os, commands

argvs = sys.argv
if (len(argvs) != 4):
    print 'Number of argv is incorrect'
    quit()

out = open('query.tmp', 'w')
out.write(argvs[1])
out.close()

os.system('mv ' + argvs[2] + ' ' + argvs[2] + '.nt')
stdout = commands.getoutput('sparql --query=query.tmp --data=' + argvs[2] + '.nt -results=TSV')
os.system('mv ' + argvs[2] + '.nt ' + argvs[2])

out = open(argvs[3], 'w')
out.write(stdout)
out.close()
