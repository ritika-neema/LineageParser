from google.cloud import storage
import json
import logging
import default_config_properties as config


def main_parser(test_bucket, config_input_gs_path, lineage_output_gs_path):
    '''Function to parse source/target properties and datasets list from config input file and/or data lineage output'''
    # Flag to mandate dataset_list in config input file uploaded by user
    mandate_dataset_list_in_input = True
    if not config_input_gs_path and not lineage_output_gs_path:
        raise Exception("Missing lineage output and input config file. \
    No information about source and target datasets could be extracted. \
    Please provide atleast one of them in GCS bucket and add path in metadata.txt")
    if lineage_output_gs_path:
        source_datasets, target_datasets = lineage_parser(
            test_bucket, lineage_output_gs_path)
        mandate_dataset_list_in_input = False
    if config_input_gs_path:
        source_info, target_info = config_input_parser(test_bucket, config_input_gs_path,
                                                       mandate_dataset_flag=mandate_dataset_list_in_input)
    else:
        # TO_DO: update property from config input
        source_info = {}
        target_info = {}
        for source_type in source_datasets.keys():
            source_info[source_type] = {}
            source_info[source_type]["dataset_list"] = source_datasets[source_type]
            source_info[source_type]["properties"] = config.get_default_source_properties(
                source_type)

        for target_type in target_datasets.keys():
            target_info[target_type] = {}
            target_info[target_type]["dataset_list"] = target_datasets[target_type]
            target_info[target_type]["properties"] = config.get_default_source_properties(
                target_type)

    return source_info, target_info


##################################  HELPER FUNCTIONS##################################


def read_json_from_gcs(bucket: str, file_path: str) -> dict:
    '''Function to read blob on GCS as JSON'''
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(file_path)
    raw_config = blob.download_as_bytes()
    config = json.loads(raw_config)
    return config


def config_input_parser(test_bucket: str, config_input_gs_path: str, mandate_dataset_flag: bool):
    '''Function to parse source/target properties and datasets list from input config uploaded by user'''
    config_input_json = read_json_from_gcs(test_bucket, config_input_gs_path)
    source_info = config_input_json["source_datasets"]
    target_info = config_input_json["target_datasets"]

    # If dataset_list is mandated in input config, check whether it is present, else raise Exception
    if mandate_dataset_flag:
        if not any(["dataset_list" in source_info[type] for type in source_info.keys()]):
            raise Exception("Please provide source dataset list in input config file. \
                        Else provide data lineage output")
        if not any(["dataset_list" in source_info[type] for type in source_info.keys()]):
            raise Exception("Please provide source dataset list in input config file. \
                        Else provide data lineage output")

    return source_info, target_info


def lineage_parser(test_bucket: str, lineage_output_gs_path: str):
    '''Function to parse source and target datasets list from data lineage output'''
    data_lineage_output = read_json_from_gcs(
        test_bucket, lineage_output_gs_path)
    sources, targets = {}, {}
    for events in data_lineage_output:
        event_sources = events["sources"]
        sources = lineage_collate_datasets(event_sources, "sources")
        event_targets = events["output"]
        targets = lineage_collate_datasets(event_targets, "output")
    return sources, targets


def lineage_collate_datasets(datasets_json: json, source_target: str):
    output = {}
    if not datasets_json:
        logging.warning(
            f"No datasets provided in data lineage output for {source_target}")
        return output
    for dataset in datasets_json:
        type = dataset["kind"]
        if type not in output:
            output[type] = []
        output[type].append(dataset["identifier"])
    return output

