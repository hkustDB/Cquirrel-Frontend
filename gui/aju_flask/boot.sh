#!/bin/sh

source venv/bin/activate

export FLASK_APP=aju_gui.py
export FLASK_DEBUG=1

flask run

