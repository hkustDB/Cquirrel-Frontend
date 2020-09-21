#!/bin/bash

python3 -m venv venv
. venv/bin/activate

export FLASK_APP=run.py
export FLASK_ENV=development
python3 -m flask run 


