from flask import Flask, request, render_template, redirect, url_for, send_file, send_from_directory
from werkzeug.utils import secure_filename

from flask_bootstrap import Bootstrap
from flask_socketio import SocketIO, send, emit

import asyncio
from asyncio.subprocess import PIPE
from asyncio import create_subprocess_shell

import os
import subprocess
import json

app = Flask(__name__)
Bootstrap(app)

app.config.from_pyfile('./conf/settings.cfg')
app.config['JSON_FILE_UPLOAD_PATH'] = os.path.join(app.config['GUI_FLASK_PATH'], 'uploads')
app.config['GENERATED_JAR_PATH'] = os.path.join(app.config['GUI_FLASK_PATH'], 'jar')
app.config['GENERATED_JAR_FILE'] = os.path.join(app.config['GUI_FLASK_PATH'], 'jar/generated.jar')
app.config['CODEGEN_FILE'] = os.path.join(app.config['GUI_FLASK_PATH'], 'jar/codegen-1.0-SNAPSHOT.jar')
app.config['CODEGEN_LOG_PATH'] = os.path.join(app.config['GUI_FLASK_PATH'], 'log')
app.config['CODEGEN_LOG_FILE'] = os.path.join(app.config['GUI_FLASK_PATH'], 'log/codegen.log')

socketio = SocketIO(app)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/', methods=['POST'])
def upload_json_file():
    f = request.files['json_file']
    uploaded_json_filename = secure_filename(f.filename)
    uploaded_json_file_save_path = os.path.join(app.config['JSON_FILE_UPLOAD_PATH'], uploaded_json_filename)       

    # check if the uploaded file is empty
    if uploaded_json_filename == '' :
        return render_template('index.html', result = "json file is empty")
        
    # save the json file to server
    f.save(uploaded_json_file_save_path)

    # check if the uploaded file is json file
    if not is_json_file(uploaded_json_file_save_path):
        # remove the uploaded non json file
        if os.path.exists(uploaded_json_file_save_path):
            os.remove(uploaded_json_file_save_path)
        return render_template('index.html', result = "uploaded file is not json file")
    
    # call the codegen to generate a jar file
    cmd_str = 'java -jar' + ' ' \
        + app.config['CODEGEN_FILE'] + ' ' \
        + uploaded_json_file_save_path + ' ' \
        + app.config['GENERATED_JAR_PATH']

    ret = subprocess.run(cmd_str, shell=True, capture_output=True)
    codegen_log_stdout = str(ret.stdout, encoding = "utf-8") + "\n"
    codegen_log_stderr = str(ret.stderr, encoding = "utf-8") + "\n"
    codegen_log_result = codegen_log_stdout + codegen_log_stderr
    with open("./log/codegen.log", "w") as f:
        f.write(codegen_log_result)

    # remove the uploaded file
    if os.path.exists(uploaded_json_file_save_path):
        os.remove(uploaded_json_file_save_path)

    return render_template('index.html', codegen_log_result = codegen_log_result)


def is_json_file(the_file):
    with open(the_file, 'r') as f:
        data = f.read()
    try:
        json_obj = json.loads(data)
    except ValueError:
        return False
    return True


def run_flink_task(filename):
    if filename == '':
        ret = subprocess.CompletedProcess(args='', returncode=1, stdout="filename is null.")
        return ret

    generated_jar_file_path = os.path.join(app.config['GENERATED_JAR_PATH'], filename)
    if not os.path.exists(generated_jar_file_path):
        ret = subprocess.CompletedProcess(args='', returncode=1, stdout="generated jar does not exist.")
        return ret

    generated_jar_para = ""
    flink_command_path = os.path.join(app.config['FLINK_HOME_PATH'], "bin/flink")
    cmd_str = flink_command_path + " run " + generated_jar_file_path + " " + generated_jar_para

    ret = subprocess.run(cmd_str, shell=True, capture_output=True)
    return ret


@app.route("/download_log")
def download_log_file():
    if os.path.exists(app.config['CODEGEN_LOG_FILE']):
        return send_from_directory(app.config['CODEGEN_LOG_PATH'], 'codegen.log', as_attachment=True)
        


if __name__ == '__main__':
    socketio.run(app)


