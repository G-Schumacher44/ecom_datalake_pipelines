select
  sv.product_id,
  date(sv.order_date) as order_date,
  avg(sv.velocity_avg) as sales_velocity_7d,
  max(sv.trend_signal) as trend_signal
from {{ ref('int_sales_velocity') }} sv
group by 1, 2
