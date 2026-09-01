select
    "InvoiceLineId"
    , "InvoiceId"
    , "TrackId"
    , "UnitPrice"
    , "Quantity"
from {{ source('chinook', 'InvoiceLine') }}
