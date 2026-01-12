select
  ap.acquisition_channel,
  date(ap.order_date) as order_date,
  countif(ap.is_recovered) as recovered_orders,
  count(*) as total_orders
from {{ source('silver', 'int_attributed_purchases') }} ap
group by 1, 2
