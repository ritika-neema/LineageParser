#!usr/bin/python
from main_parser import main_parser
from hive_table_clone_wrapper_edited import hive_clone_wrapper

TEST_BUCKET_ID = "rne-gcs"
PROD_BUCKET_ID = ""
LINEAGE_OUTPUT_GS_PATH = "data-lineage-op.json"
CONFIG_INPUT_GS_PATH = "sample-config-full.json"

if __name__ == "__main__":

    #To-Do: Copy files from artifactory to local
    source_info, target_info = main_parser(TEST_BUCKET_ID, CONFIG_INPUT_GS_PATH, LINEAGE_OUTPUT_GS_PATH)
    hive_clone_wrapper(source_info["hive"], target_info["hive"])

