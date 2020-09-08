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


    # temporarily show the result
    output = subprocess.check_output(cmd_str, shell=True)
    with open(uploaded_json_file_save_path, "r") as jf:
        json_tmp = jf.read()
    result = str(output, encoding = "utf-8") + "\n" + json_tmp

    # remove the uploaded file
    if os.path.exists(uploaded_json_file_save_path):
        os.remove(uploaded_json_file_save_path)

    return render_template('index.html', result = result)
    # return redirect(url_for('upload_json_file'))
    # return render_template('index.html', result = "null")


def is_json_file(the_file):
    with open(the_file, 'r') as f:
        data = f.read()
    try:
        json_obj = json.loads(data)
    except ValueError:
        return False
    return True


if __name__ == '__main__':
    app.run(debug=True)

