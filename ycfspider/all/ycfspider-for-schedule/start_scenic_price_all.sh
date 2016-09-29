#!/usr/bin/env bash
count=0
while [ $count -lt $1 ];
do
  sleep 2 
  nohup python ctrip_pc_scenic_price_start.py >/dev/null 2>&1 &
  nohup python qunar_pc_scenic_price_start.py >/dev/null 2>&1 &
  nohup python lvmama_pc_scenic_price_start.py >/dev/null 2>&1 &
  nohup python meituan_app_scenic_price_start.py >/dev/null 2>&1 &
  let count+=1
done
