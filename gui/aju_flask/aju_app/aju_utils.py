import os
import subprocess
import json
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

    # TODO check the flink is running or not

    if filename == '':
        ret = subprocess.CompletedProcess(args='', returncode=1, stdout="filename is null.")
        return ret

    generated_jar_file_path = os.path.join(config.GENERATED_JAR_PATH, filename)
    if not os.path.exists(generated_jar_file_path):
        ret = subprocess.CompletedProcess(args='', returncode=1, stdout="generated jar does not exist.")
        return ret

    generated_jar_para = ""
    flink_command_path = os.path.join(config.FLINK_HOME_PATH, "bin/flink")
    cmd_str = flink_command_path + " run " + generated_jar_file_path + " " + generated_jar_para

    print(cmd_str)
    ret = subprocess.run(cmd_str, shell=True, capture_output=True)
    return ret
