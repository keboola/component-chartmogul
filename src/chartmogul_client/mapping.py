pkeys_mapping = {
    "customers": ["id", "uuid"],
    "activities": ["uuid"],
    "customers_subscriptions": ["id", "customers_uuid"],
    "key_metrics": ["date"],
    "invoices": ["uuid"],
    "invoices_line_items": ["uuid", "JSON_parentId"],
    "invoices_transactions": ["uuid", "JSON_parentId"]
}
