select *
from {{ source('bronze', 'orders') }}
