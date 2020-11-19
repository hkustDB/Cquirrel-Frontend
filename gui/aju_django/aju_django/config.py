import os
from . import settings

UPLOAD_DIR = settings.BASE_DIR / 'upload'
JAR_DIR = settings.BASE_DIR / 'jar'
GENERATED_CODE_DIR = JAR_DIR / 'generated-code'
GENERATED_JAR = JAR_DIR / 'generated-code/target/generated-code-1.0-SNAPSHOT-jar-with-dependencies.jar'
CODEGEN_JAR = JAR_DIR / 'codegen.jar'
CODEGEN_LOG = JAR_DIR / 'codegen.log'
RESOURCES_DIR = settings.BASE_DIR / 'resources'
INPUT_DATA_FILE = RESOURCES_DIR / 'input_data_q6_all_insert.csv'
OUTPUT_DATA_FILE = RESOURCES_DIR / 'output_data_q6_all_insert.csv'

FLINK_HOME = os.environ.get('FLINK_HOME') or '/Users/chaoqi/Programs/flink-1.11.2'
REMOTE_FLINK = False
REMOTE_FLINK_URL = '47.93.121.10:8081'