
# USAGE: $ sh editheader.sh "col1 col2 col3" input.txt output.txt 0

# 1 HEADER
# 2 INPUT_FILE
# 3 OUTPUT_FILE
# 4 DELETE_FLG

if [ $4 == 0 ]; then
  echo "The first row will NOT be deleted."
  echo -e "$1" | cat - $2 > $3
else
  echo "The first row will be deleted."
  echo -e "$1" | cat - $2 | awk 'NR!=2 {print}' > $3
fi
