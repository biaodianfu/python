  #!/usr/bin/env bash
nohup python write_cookie.py  >/dev/null 2>&1 &
count=0
while [ $count -lt $1 ];
do
  sleep 2 
  nohup python ctrip_m_hotel_price_start.py >/dev/null 2>&1 &
  nohup python qunar_pc_hotel_price_start.py >/dev/null 2>&1 &
  nohup python elong_pc_hotel_price_start.py >/dev/null 2>&1 &
  let count+=1
done
