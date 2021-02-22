#!/usr/bin/env bash
pip3 install -r /opt/airflow/entrypoints/requirements.txt --user
airflow db init
airflow webserver