import json
import os
import logging
import shutil
import subprocess
from pathlib import Path
from aju_django import settings
from aju_django import config

logger = logging.getLogger(__name__)


def init_dir():
    if not config.RESOURCES_DIR.exists():
        config.RESOURCES_DIR.mkdir()
    if not config.JAR_DIR.exists():
        config.JAR_DIR.mkdir()
    if not config.UPLOAD_DIR.exists():
        config.UPLOAD_DIR.mkdir()


def is_json_file(file):
    with open(file, 'r') as f:
        data = f.read()
    try:
        json.loads(data)
    except ValueError:
        return False
    return True


def validate_uploaded_json_file(file):
    p = settings.BASE_DIR / 'tmp'
    if not p.exists():
        Path.mkdir(p)
    pf = str((p / 'tmp.json').absolute())
    if (p / 'tmp.json').exists():
        (p / 'tmp.json').unlink()

    if file.multiple_chunks():
        with open(pf, 'wb+') as destination:
            for chunk in file.chunks:
                destination.write(chunk)
    else:
        with open(pf, 'wb') as destination:
            destination.write(file.read())

    res = is_json_file(pf)
    os.remove(pf)
    return res


def store_uploaded_json_file(file):
    if not config.UPLOAD_DIR.exists():
        config.UPLOAD_DIR.mkdir()
    filepath = config.UPLOAD_DIR / file.name
    if filepath.exists():
        filepath.unlink()
    if file.multiple_chunks():
        with open(filepath, 'wb+') as destination:
            for chunk in file.chunks:
                destination.write(chunk)
    else:
        with open(filepath, 'wb') as destination:
            destination.write(file.read())
    logger.info('uploaded json file is stored at ' + str(filepath))
    return filepath


def remove_uploaded_json_file(filepath):
    if filepath.exists():
        filepath.unlink()
        logger.info('remove uploaded json file.')
        return True
    else:
        logger.error('remove uploaded json file failed.')
        return False


def remove_generated_code_dir():
    generated_code_dir = config.GENERATED_CODE_DIR
    if generated_code_dir.exists():
        shutil.rmtree(generated_code_dir.absolute())
        logging.info('remove generated-code directory.')


def run_codegen_task(uploaded_json_file_path):
    if not config.CODEGEN_JAR.exists():
        codegen_log_result = 'codegen jar does not exit.'
        logger.error(codegen_log_result)
        return codegen_log_result
    if not config.INPUT_DATA_FILE.exists():
        codegen_log_result = 'input data file does not exit.'
        logger.error(codegen_log_result)
        return codegen_log_result
    if not config.OUTPUT_DATA_FILE.exists():
        codegen_log_result = 'output data file does not exit.'
        logger.error(codegen_log_result)
        return codegen_log_result

    cmd_str = 'java -jar' + ' ' \
              + str(config.CODEGEN_JAR.absolute()) + ' ' \
              + str(uploaded_json_file_path.absolute()) + ' ' \
              + str(config.JAR_DIR.absolute()) + ' ' \
              + 'file://' + str(config.INPUT_DATA_FILE.absolute()) + ' ' \
              + 'file://' + str(config.OUTPUT_DATA_FILE.absolute()) + ' ' + 'file'

    logger.info("codegen command: " + cmd_str)
    ret = subprocess.run(cmd_str, shell=True, capture_output=True)
    codegen_log_stdout = str(ret.stdout, encoding="utf-8") + "\n"
    codegen_log_stderr = str(ret.stderr, encoding="utf-8") + "\n"
    codegen_log_result = codegen_log_stdout + codegen_log_stderr
    if ret.returncode != 0:
        logger.error(codegen_log_result)
    with open(config.CODEGEN_LOG, "w") as f:
        f.write(codegen_log_result)

    remove_uploaded_json_file(uploaded_json_file_path)

    return codegen_log_result


def run_flink_task(filename):

    if filename == '':
        ret = subprocess.CompletedProcess(args='', returncode=1, stdout="filename is null.")
        return ret

    generated_jar_file_path = os.path.join(config.JAR_DIR, filename)
    if not os.path.exists(generated_jar_file_path):
        ret = subprocess.CompletedProcess(args='', returncode=1, stdout="generated jar does not exist.")
        return ret

    generated_jar_para = ""
    flink_command_path = os.path.join(config.FLINK_HOME, "bin/flink")
    if config.REMOTE_FLINK:
        cmd_str = flink_command_path + " run " + " -m " + config.REMOTE_FLINK_URL + " " + generated_jar_file_path + " " + generated_jar_para
    else:
        cmd_str = flink_command_path + " run " + generated_jar_file_path + " " + generated_jar_para

    logging.info("flink command: " + cmd_str)
    ret = subprocess.run(cmd_str, shell=True, capture_output=True)
    aju_app.background_send_kafka_data_thread()
    return ret


def handle_uploaded_json_file(file):
    result = {}
    if not validate_uploaded_json_file(file):
        result['upload_file_result'] = 'upload file is not json.'
        return result
    else:
        result['upload_file_result'] = file.name + ' upload success.'
    uploaded_json_file_path = store_uploaded_json_file(file)
    codegen_log_result = run_codegen_task(uploaded_json_file_path)
    result['codegen_log_result'] = codegen_log_result



    return result
