#!/bin/bash
# for file in `ls test`
# do
# 	coverage run -p $file
# done
coverage run -p test_run_flink.py
coverage run -p test_upload.py
coverage run -p test_validation.py
coverage combine
coverage report
