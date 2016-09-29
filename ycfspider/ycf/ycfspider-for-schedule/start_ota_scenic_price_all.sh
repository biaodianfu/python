#!/usr/bin/env bash
count=0
while [ $count -lt $1 ];
do
  sleep 2 
  nohup python ctrip_ota_pc_ebooking_hotel_info_start.py >/dev/null 2>&1 &
  nohup python elong_ota_pc_ebooking_hotel_info_start.py >/dev/null 2>&1 &
  nohup python qunar_ota_pc_hota_hotel_info_start.py >/dev/null 2>&1 &
  nohup python qunar_ota_pc_zcf_hotel_info_start.py >/dev/null 2>&1 &
  let count+=1
done

