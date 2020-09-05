from flask import Flask, request, render_template, redirect, url_for
from werkzeug.utils import secure_filename
import os
import subprocess

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
        flink_address = './'
        cmd_str = 'java -jar' + ' ' + codegen_file_path + ' ' + json_file_save_path + ' ' + flink_address

        output = subprocess.check_output(cmd_str, shell=True)
        print(output)
        # os.system(cmd_str)

        return render_template('upload.html', result = output)
        # return redirect(url_for('upload_json_file'))
    return render_template('upload.html')


if __name__ == '__main__':
    app.run(debug=True)

