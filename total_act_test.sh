#!/bin/bash
#rank $1
#nc $2
python dbstate.py > $1-state.txt
for(( i = 1 ; i <= $2 ; i++ ))
{
	if((i % 5 == $1))
	then
		python act_test.py < ./xact-files/$i.txt > /dev/null 2> ./xres/$i-res.txt &
	fi
}