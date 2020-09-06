from flask import Flask, request, render_template, redirect, url_for
from werkzeug.utils import secure_filename
import os
import subprocess
import json

app = Flask(__name__)

@app.route('/')
def hello_world():
    return render_template('index.html')


@app.route('/upload', methods=['POST', 'GET'])
def upload_json_file():
    if request.method == 'POST':
        f = request.files['json_file']
        json_file_save_path = './uploads/' + secure_filename(f.filename)
        f.save(json_file_save_path)
        codegen_file_path = './codegen-1.0-SNAPSHOT.jar'
        generated_jar_path = './generated_jar'
        cmd_str = 'java -jar' + ' ' + codegen_file_path + ' ' + json_file_save_path + ' ' + generated_jar_path

        output = subprocess.check_output(cmd_str, shell=True)

        return render_template('upload.html', result = str(output))
        # return redirect(url_for('upload_json_file'))
    return render_template('upload.html', result = "null")


def is_json_file(the_file):
    try:
        json_obj = json.loads(the_file)
    except ValueError:
        return False
    return True


if __name__ == '__main__':
    app.run(debug=True)

