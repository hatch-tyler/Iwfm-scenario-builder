#!/usr/bin/env python
# coding: utf-8

# ## 1980 Streamflow Depletion Scenario
# ---
# Generates the time-series pumping file and well specifications file for each transfer project

import os
import matplotlib as mpl
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from scenario_builder.scenario_builder import scenario_builder

mpl.use('Agg')

def print_info():
    print(f"Current working directory:{os.getcwd()}")
    print(f"WORKING_PATH environment variable:{os.getenv('WORKING_PATH')}")

# input information
working_path = "/data"
gw_path = "Simulation/Groundwater"
ws_file = "C2VSimFG_WellSpec.dat"
pr_file = "C2VSimFG_PumpRates.dat"
wn_file = "well_names_and_owners.csv"
prj_file = "TransferProjects.csv"
start_date = "1973-10-31"
scenario_year = 1980
pumping_duration = 6
output_path = "FilesToCopy"
qa_path = "QA"

# set full paths to projects file
projects_file = os.path.join(working_path, prj_file)

# read file containing list of projects
projects = pd.read_csv(projects_file)
transfer_projects = projects["Project"].tolist()

with DAG(
    dag_id="run_model_parallel",
    schedule="0 0 * * *",
    start_date=datetime(2022, 12, 11),
    catchup=False,
    dagrun_timeout=timedelta(days=1),
    tags=["iwfm"]
) as dag:
    check_info = PythonOperator(
        task_id="GetInfo",
        python_callable=print_info,
        execution_timeout=timedelta(minutes=1),
        dag=dag,
    )

    run_this_last = EmptyOperator(
        task_id="run_this_last",
        dag=dag
    )

    # loop through each project to generate the pumping timeseries file
    for proj_no, proj in enumerate(transfer_projects, start=1):
        
        build_scenarios = PythonOperator(
            task_id=f"build_scenario_{proj_no}",
            python_callable=scenario_builder,
            op_kwargs={
                "project": proj,
                "projects": projects,
                "working_path": working_path,
                "gw_path": gw_path,
                "ws_file": ws_file,
                "pr_file": pr_file,
                "wn_file": wn_file,
                "start_date": start_date,
                "scenario_year": scenario_year,
                "pumping_duration": pumping_duration,
                "output_path": output_path,
                "qa_path": qa_path,
            },
            execution_timeout=timedelta(minutes=30),
            dag=dag)

    check_info >> build_scenarios >> run_this_last

if __name__ == "__main__":
    print("Testing DAG")
    dag.test()