import os
import json
import subprocess
import logging

import aju_app
import config


def is_json_file(the_file):
    with open(the_file, 'r') as f:
        data = f.read()
    try:
        json_obj = json.loads(data)
    except ValueError:
        return False
    return True


def run_flink_task(filename):
    from config import REMOTE_FLINK
    from config import REMOTE_FLINK_URL

    if filename == '':
        ret = subprocess.CompletedProcess(args='', returncode=1, stdout="filename is null.")
        return ret

    generated_jar_file_path = os.path.join(config.GENERATED_JAR_PATH, filename)
    if not os.path.exists(generated_jar_file_path):
        ret = subprocess.CompletedProcess(args='', returncode=1, stdout="generated jar does not exist.")
        return ret

    generated_jar_para = ""
    flink_command_path = os.path.join(config.FLINK_HOME_PATH, "bin/flink")
    if REMOTE_FLINK:
        cmd_str = flink_command_path + " run " + " -m " + REMOTE_FLINK_URL + " " + generated_jar_file_path + " " + generated_jar_para
    else:
        cmd_str = flink_command_path + " run " + generated_jar_file_path + " " + generated_jar_para

    logging.info("flink command: " + cmd_str)
    ret = subprocess.run(cmd_str, shell=True, capture_output=True)
    aju_app.background_send_kafka_data_thread()
    return ret


def run_codegen_to_generate_jar(uploaded_json_file_save_path):
    cmd_str = 'java -jar' + ' ' \
              + config.CODEGEN_FILE + ' ' \
              + uploaded_json_file_save_path + ' ' \
              + config.GENERATED_JAR_PATH + ' ' \
              + 'file://' + config.INPUT_DATA_FILE + ' ' \
              + 'file://' + config.OUTPUT_DATA_FILE + ' ' + 'file'

    logging.info("codegen command: " + cmd_str)
    ret = subprocess.run(cmd_str, shell=True, capture_output=True)
    codegen_log_stdout = str(ret.stdout, encoding="utf-8") + "\n"
    codegen_log_stderr = str(ret.stderr, encoding="utf-8") + "\n"
    codegen_log_result = codegen_log_stdout + codegen_log_stderr
    with open("./log/codegen.log", "w") as f:
        f.write(codegen_log_result)

    # remove the uploaded file
    if os.path.exists(uploaded_json_file_save_path):
        os.remove(uploaded_json_file_save_path)

    return codegen_log_result
