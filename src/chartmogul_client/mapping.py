pkeys_mapping = {
    "customers": ["id", "uuid"],
    "customers_data_source_uuids": ["JSON_parentId"],
    "customers_external_ids": ["JSON_parentId"],
    "activities": ["uuid"],
    "customers_subscriptions": ["id", "customers_uuid"],
    "key_metrics": ["date"],
    "invoices": ["uuid"],
    "invoices_line_items": ["uuid", "JSON_parentId"],
    "invoices_transactions": ["uuid", "JSON_parentId"]
}
