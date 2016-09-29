#!/bin/bash
count=0
echo $1
echo $2
while [ $count -lt $2 ];
do
  sleep 2
  python $1 &
  let count+=1
done
wait
#for((i=0;i<$2;i++));
#do
#  j=$(echo "$i+1" | bc -l)
#  echo $j
#  wait %$j
#  echo $?
#done 
