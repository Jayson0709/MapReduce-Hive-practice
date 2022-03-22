select ss_sold_date_sk, sum(ss_net_paid_inc_tax) as total_tax from store_sales_40
where ss_sold_date_sk >= 2451392
  and ss_sold_date_sk <= 2451894
  and ss_sold_date_sk is not null
  and ss_net_paid_inc_tax is not null
group by ss_sold_date_sk
order by total_tax desc
    limit 10;