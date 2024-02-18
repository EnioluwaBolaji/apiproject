SELECT ("unitPrice" * "quantity" ) as Revenue
from {{ ref('stgordersdetails') }}