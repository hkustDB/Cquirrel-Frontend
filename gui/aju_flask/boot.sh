#!/bin/sh

#source venv/bin/activate

export FLASK_APP=aju_gui.py
export FLASK_DEBUG=1

#flask run

gunicorn --worker-class eventlet -w 1 -b 0.0.0.0:5000 aju_gui:app

