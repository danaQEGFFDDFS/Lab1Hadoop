#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify output file!'
    exit 1
fi

SIGNIFICANT_ID=("debug" "info" "notice" "warning" "warn" "err" "error" "crit" "alert"  "emerg" "panic")

START_DATE="FEB 5 "
START_HOUR=10
START_MINUTE=10
END_SECOND="10"
TEST_STRING="SOME TEST SITUATION"

rm -rf input
mkdir input

for i in {1..13}
	do
	  for j in {1..40}
	    do
	      RESULT="$START_DATE$(($START_HOUR+$i)):$((START_MINUTE+$j)):$END_SECOND - ${SIGNIFICANT_ID[$((RANDOM % ${#SIGNIFICANT_ID[*]}))]} $TEST_STRING"
	      echo $RESULT >> input/$1.1
	    done
	done


