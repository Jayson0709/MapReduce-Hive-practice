select S.s_store_sk, SS.total_net_paid, S.s_floor_space from store_40 S join
(
    select ss_store_sk, sum(ss_net_paid) as total_net_paid from store_sales_40
    where ss_sold_date_sk >= 2451146
      and ss_sold_date_sk <= 2452268
      and ss_store_sk is not null
      and ss_net_paid is not null
    group by ss_store_sk
) SS
on S.s_store_sk = SS.ss_store_sk
order by S.s_floor_space desc, SS.total_net_paid desc
limit 10;
