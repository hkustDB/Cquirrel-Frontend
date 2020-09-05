# AJU-GUI
This is the gui part of the AJU project.

# Environment
Ubuntu 20.04.1 LTS

Python 3.8.2

Flask 1.1.2

Werkzeug 1.0.1
# Flask Installation

## Virtual Environment
### Create an environment
Create a project folder and a venv folder within:

```
$ mkdir gui/flask
$ cd gui/flask
$ python3 -m venv venv
```

### Activate the environment
Before you work on your project, activate the corresponding environment:
```
$ . venv/bin/activate
```

## Install Flask
Within the activated environment, use the following command to install Flask:
```
$ pip install Flask
```

# Flask Server Run
`FLASK_APP` is the main python file to run.
`FLASK_ENV` can set the debug mode when it is assigned `development`. Then in this mode, we can change the server code without restart the flask service.

So the "gui/flask/start.sh" file sets the above two variables and runs the flask.