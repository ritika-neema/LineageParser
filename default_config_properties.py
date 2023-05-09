def get_default_source_properties(type: str) -> dict:
    properties = {
        "hive": {
            "max_recent_hive_partitions": 5,
            "data_copy": True,
            "metadata_store": "global"
        },
        "bigquery": {
            "retention_period": 30
        }
    }
    if type not in properties:
        raise Exception(f"Unsupported source provided in function get_default_source_properties as {type}")
    return properties[type]


def get_default_target_properties(type: str) -> dict:
    properties = {
        "hive": {
            "overwrite": True,
            "metadata_store": "local",
            "persist_metadata": True,
            "persist_metadata_gcs_path": "<folder path on test bucket>",
            "persist_datafiles": True,
            "persist_table": True
        }
    }
    if type not in properties:
        raise Exception(f"Unsupported target provided in function get_default_target_properties as {type}")
    return properties[type]

