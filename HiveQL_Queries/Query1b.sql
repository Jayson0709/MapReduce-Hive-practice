select ss_item_sk, sum(ss_quantity) as sales from store_sales_40
where ss_sold_date_sk >= 2451146
  and ss_sold_date_sk <= 2452268
  and ss_item_sk is not null
  and ss_quantity is not null
group by ss_item_sk
order by sales desc
limit 10;
