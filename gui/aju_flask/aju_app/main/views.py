from . import main
from .. import aju_utils
import config
from flask import render_template, request, send_from_directory
from werkzeug.utils import secure_filename

import os
import shutil
import subprocess
import logging


@main.route('/')
def index():
    return render_template('index.html')


@main.route('/', methods=['POST'])
def upload_json_file():
    f = request.files['json_file']
    uploaded_json_filename = secure_filename(f.filename)
    uploaded_json_file_save_path = os.path.join(config.JSON_FILE_UPLOAD_PATH, uploaded_json_filename)

    # check if the upload dir exists or not
    if not os.path.exists(config.JSON_FILE_UPLOAD_PATH):
        os.makedirs(config.JSON_FILE_UPLOAD_PATH)

    # check if the uploaded file is empty
    if uploaded_json_filename == '':
        return render_template('index.html', uploaded_result="The uploaded json file is empty. Please upload again.")

    # save the json file to server
    f.save(uploaded_json_file_save_path)

    # check if the uploaded file is json file
    if not aju_utils.is_json_file(uploaded_json_file_save_path):
        # remove the uploaded non json file
        if os.path.exists(uploaded_json_file_save_path):
            os.remove(uploaded_json_file_save_path)
        return render_template('index.html',
                               uploaded_result="The uploaded file is not a json file. Please upload again.")

    # remove the older generated-code directory
    if os.path.isdir(config.GENERATED_CODE_DIR):
        shutil.rmtree(config.GENERATED_CODE_DIR)
        logging.info('remove the generated-code directory.')

    # call the codegen to generate a jar file
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

    aju_utils.run_flink_task(config.GENERATED_JAR_FILE)
    return render_template('index.html', codegen_log_result=codegen_log_result,
                           uploaded_result="The json file uploaded successfully.")


@main.route("/download_log")
def download_log():
    if os.path.exists(config.CODEGEN_LOG_FILE):
        return send_from_directory(config.CODEGEN_LOG_PATH, 'codegen.log', as_attachment=True)


@main.route("/download_generated_jar")
def download_generated_jar():
    if os.path.exists(config.GENERATED_JAR_FILE):
        return send_from_directory(config.GENERATED_JAR_PATH, 'generated.jar', as_attachment=True)
