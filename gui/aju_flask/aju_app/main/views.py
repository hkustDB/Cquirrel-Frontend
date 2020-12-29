import aju_app
from . import main
from .. import aju_utils
import config
from flask import render_template, request, send_from_directory, flash, redirect, url_for
from werkzeug.utils import secure_filename

import os
import shutil
import logging
import threading


@main.route('/')
def index():
    aju_utils.clean_codegen_log_and_generated_jar();
    return render_template('index.html')


@main.route('/', methods=['POST'])
def upload_json_file():
    aju_app.stop_send_data_thread()

    f = request.files['json_file']
    if f.filename == "":
        return render_template('index.html', uploaded_result="The uploaded file is not selected. Please select the file.")

    uploaded_json_filename = secure_filename(f.filename)
    query_idx = aju_utils.get_query_idx(uploaded_json_filename)
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
                               uploaded_result="The format of uploaded file is not a correct json format. Please upload again.")

    # remove the older generated-code directory
    if os.path.isdir(config.GENERATED_CODE_DIR):
        shutil.rmtree(config.GENERATED_CODE_DIR)
        logging.info('remove the generated-code directory.')

    # call the codegen to generate a jar file
    codegen_log_result, retcode = aju_utils.run_codegen_to_generate_jar(uploaded_json_file_save_path, query_idx)

    aju_app.send_codegen_log_data_to_client(codegen_log_result)

    # test if flink cluster is running
    if not aju_utils.is_flink_cluster_running():
        return render_template('index.html', codegen_log_content=codegen_log_result,
                               uploaded_result="The json file uploaded successfully.",
                               flink_status_result='Flink cluster is not running, please start flink cluster!')

    # call the flink to run the generated_jar
    t = threading.Thread(target=aju_utils.run_flink_task, args=(config.GENERATED_JAR_FILE, query_idx))
    t.start()

    logging.info("codegen_log_result: " + codegen_log_result)
    if retcode == 0:
        return render_template('index.html', codegen_log_content=codegen_log_result,
                               uploaded_result="The json file uploaded successfully.",
                               flink_status_result='Start to run Flink job, please wait a moment.')
    else:
        flash("Codegen failed!")
        return render_template('index.html', codegen_log_content=codegen_log_result,
                               uploaded_result="The json file uploaded successfully.")


@main.route("/download_codegen_log")
def download_codegen_log():
    if os.path.exists(config.CODEGEN_LOG_FILE):
        return send_from_directory(config.CODEGEN_LOG_PATH, 'codegen.log', as_attachment=True)
    else:
        flash("Codegen log does not exists.")
        return redirect(url_for("main.index"))


@main.route("/download_generated_jar")
def download_generated_jar():
    if os.path.exists(config.GENERATED_JAR_FILE):
        return send_from_directory(config.GENERATED_JAR_PATH, 'generated.jar', as_attachment=True)
    else:
        flash("Generated Jar does not exists.")
        return redirect(url_for("main.index"))


@main.route('/settings')
def settings():
    return render_template('settings.html')


@main.route('/about')
def about():
    return render_template('about.html')


@main.route('/contact')
def contact():
    return render_template('contact.html')
