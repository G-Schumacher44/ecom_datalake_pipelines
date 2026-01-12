select
  rf.region,
  date(rf.order_date) as order_date,
  sum(rf.total_price) as gross_revenue,
  sum(rf.tax_amount) as tax_amount,
  sum(rf.net_revenue) as net_revenue
from {{ source('silver', 'int_regional_financials') }} rf
group by 1, 2
