#!/usr/bin/env python
# coding: utf-8

# ## 1980 Streamflow Depletion Scenario
# ---
# Generates the time-series pumping file and well specifications file for each transfer project

import os
import shutil
import matplotlib as mpl
import pandas as pd
from datetime import datetime, timedelta
from subprocess import Popen, PIPE, STDOUT

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from scenario_builder.scenario_builder import scenario_builder
from scenario_builder.utilities import make_directory
from scenario_builder.utilities import s3_download2, s3_upload, zip_model

mpl.use('Agg')

def print_info():
    """
    Print information about run environment
    """
    print(f"Current working directory:{os.getcwd()}")
    print(f"WORKING_PATH environment variable:{os.getenv('WORKING_PATH')}")
    print(f"{os.path.basename(__name__)} is located here: {os.path.abspath(os.path.dirname(__name__))}")

def run_model(scenario="Base", scenario_year=1980):
    """
    Run model

    Parameters
    ----------
    scenario : str
        Scenario ID for the model run

    scenario_year : int
        year corresponding to the scenario pumping
    """
    scenario_path = "/data/FilesToCopy"

    # retrieve scenario files from s3
    if scenario.upper() != "BASE":
        # set title for well specification file
        ws_title = f"{scenario_year}_WELLSPEC_{scenario}.DAT"

        # set title of pumping rates file for scenario
        pump_title = f"{scenario_year}_PUMPING_{scenario}.DAT"
        
        # make directory to store scenario files
        make_directory(scenario_path)
        
        print(f"Retrieving {ws_title} from S3")
        s3_download2(os.path.join(scenario_path, ws_title))

        print(f"Retrieving {pump_title} from S3")
        s3_download2(os.path.join(scenario_path, pump_title))

    # run bash script
    print("Running model...")
    # run preprocessor
    pp_path = "/data/Preprocessor"

    os.chdir(pp_path)
    print(f"Running {os.path.basename(pp_path)}...")
    bash_path = shutil.which("bash")
    bash_command = [bash_path, "-c", "/build/iwfm/PreProcessor C2VSimFG_Preprocessor.in"]
    pp = Popen(
        bash_command,
        stdout=PIPE,
        stderr=STDOUT,
        env={"LD_LIBRARY_PATH": "/opt/intel/oneapi/compiler/2022.0.2/linux/compiler/lib/intel64_lin"},
        cwd=pp_path,
    )
    if pp.stdout is not None:
        for raw_line in iter(pp.stdout.readline, b""):
            line = raw_line.decode("utf-8", errors="backslashreplace").rstrip()
            print(line)
    
    pp.wait()

    print(f"The command finished with return code: {pp.returncode}")
    if pp.returncode != 0:
        raise RuntimeError("Preprocessor did not run successfully.")
    
    print(f"Copying PreprocessorOut.bin to Simulation directory")
    shutil.copy("./..\\Simulation\\C2VSimFG_PreprocessorOut.bin", "../Simulation/C2VSimFG_PreprocessorOut.bin")
    #m = subprocess.run(["/data/run_model.sh", scenario])

    #if m.returncode != 0:
    #    print(m.stderr)
    #    raise RuntimeError("run_model script did not complete successfully.")

    #zip_name = f"/data/Scenario{scenario}.zip"
    #zip_model(zip_name, "/data/Simulation")
    #s3_upload(zip_name)


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
scenario_ids = projects["Scenario"].tolist()

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
    for scenario_id in scenario_ids:
        
        build_scenarios = PythonOperator(
            task_id=f"BuildScenario_{scenario_id:02d}",
            python_callable=scenario_builder,
            op_kwargs={
                "scenario": scenario_id,
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
            retries=2,
            execution_timeout=timedelta(minutes=30),
            dag=dag)

        run_scenarios = PythonOperator(
            task_id=f"RunScenario_{scenario_id:02d}",
            python_callable=run_model,
            op_kwargs={
                "scenario": f"{scenario_id:02d}",
                "scenario_year": scenario_year,
            },
            execution_timeout=timedelta(hours=12),
            dag=dag)
        #run_scenarios = BashOperator(
        #    task_id=f"RunScenario_{scenario_id:02d}",
        #    bash_command="/build/iwfm/PreProcessor C2VSimFG_Preprocessor.in ",
        #    env={
        #        "LD_LIBRARY_PATH": "/opt/intel/oneapi/compiler/2022.0.2/linux/compiler/lib/intel64_lin",
        #    },
        #    execution_timeout=timedelta(hours=1),
        #    cwd="/data/Preprocessor",
        #    dag=dag,
        #)

        check_info >> build_scenarios >> run_scenarios >> run_this_last

if __name__ == "__main__":
    print("Testing DAG")
    dag.test()