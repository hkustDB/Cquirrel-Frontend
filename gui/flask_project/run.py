from flask import Flask, request, render_template, redirect, url_for
from werkzeug.utils import secure_filename
import os
import subprocess
import json

app = Flask(__name__)
app.config['JSON_FILE_UPLOAD_PATH'] = './uploads/'
app.config['GENERATED_JAR_PATH'] = './generated_jar/'
app.config['CODEGEN_FILE'] = './codegen-1.0-SNAPSHOT.jar'

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/', methods=['POST'])
def upload_json_file():
    # if request.method == 'POST':
    f = request.files['json_file']
    uploaded_json_filename = secure_filename(f.filename)
    uploaded_json_file_save_path = os.path.join(app.config['JSON_FILE_UPLOAD_PATH'], uploaded_json_filename)
    if uploaded_json_filename != '' :
        f.save(uploaded_json_file_save_path)
    
    cmd_str = 'java -jar' + ' ' \
        + app.config['CODEGEN_FILE'] + ' ' \
        + uploaded_json_file_save_path + ' ' \
        + app.config['GENERATED_JAR_PATH']

    output = subprocess.check_output(cmd_str, shell=True)

    return render_template('index.html', result = str(output, encoding = "utf-8"))
    # return redirect(url_for('upload_json_file'))
    # return render_template('index.html', result = "null")


def is_json_file(the_file):
    try:
        json_obj = json.loads(the_file)
    except ValueError:
        return False
    return True


if __name__ == '__main__':
    app.run(debug=True)

