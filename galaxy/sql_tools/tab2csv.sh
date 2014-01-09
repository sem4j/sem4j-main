if [ -e $3 ]
then 
sed -e 's/\t/,/g' $1 > $2
else 
sed -e 's/\t/","/g' $1 | sed -e 's/^\|$/"/g' > $2
fi
