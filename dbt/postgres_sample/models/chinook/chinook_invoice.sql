select
    "InvoiceId"
    , "CustomerId"
    , "InvoiceDate"
    , "BillingAddress"
    , "BillingCity"
    , "BillingState"
    , "BillingCountry"
    , "BillingPostalCode"
    , "Total"
from {{ source('chinook', 'Invoice') }}
