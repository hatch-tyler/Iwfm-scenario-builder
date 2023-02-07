#!/usr/bin/env python
# coding: utf-8

# ## Run Single Base Scenario
# ---
# Performs model simulation and ETL to postgresql

import os
import shutil
import psycopg2
import matplotlib as mpl
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from subprocess import Popen, PIPE, STDOUT
from sqlalchemy import create_engine
from urllib.parse import quote

from pywfm import IWFMBudget

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from scenario_builder.scenario_builder import scenario_builder
from scenario_builder.utilities import make_directory, get_file_size
from scenario_builder.utilities import s3_download2, s3_upload2, zip_model

mpl.use("Agg")


def print_info():
    """
    Print information about run environment
    """
    print(f"Current working directory:{os.getcwd()}")
    print(f"WORKING_PATH environment variable:{os.getenv('WORKING_PATH')}")


def postgres_engine(username, password, db_name, server):
    """
    Create database engine
    """
    url_password = quote(password)
    db_connection_url = (
        f"postgresql+psycopg2://{username}:{url_password}@{server}:5432/{db_name}"
    )

    return create_engine(db_connection_url)


def gis2postgis(gis_dir, gis_ext, username, password, db_name, server):
    """
    Ingest GIS data to Postgresql
    """
    engine = postgres_engine(username, password, db_name, server)

    feature_classes = [
        os.path.join(gis_dir, f) for f in os.listdir(gis_dir) if f.endswith(gis_ext)
    ]
    for fc in feature_classes:
        fc_name = os.path.basename(fc)
        print(f"Writing {fc_name} to PostGIS...")
        tbl_name = os.path.splitext(fc_name)[0].lower()
        fc_gdf = gpd.read_file(fc)
        fc_gdf.to_postgis(tbl_name, con=engine, if_exists="replace")


def budgethdf2postgres(
    results_dir, engine, scenario, exclude_kws=["ZBudget", "Hydrographs", "Head"]
):
    """
    Load IWFM Budget HDF files to postgres
    """
    bud_files = [
        os.path.join(results_dir, f)
        for f in os.listdir(results_dir)
        if f.endswith("hdf") and not any(kw in f for kw in exclude_kws)
    ]
    for f in bud_files:
        with IWFMBudget(f) as bud:
            # generate table name for database
            bud_fname = os.path.basename(bud.budget_file_name)
            print(f"Writing budget {bud_fname} to Postgresql...")

            bud_tbl_name = os.path.splitext(bud_fname)[0].lower()

            # get location names for budgets
            locations = bud.get_location_names()

            # combine all budget locations into a single dataframe
            for i, location in enumerate(locations, start=1):
                print(f"Writing {location} to postgresql")
                bud_df = bud.get_values(i)

                # extra steps for stream diversion budget
                if {'Actual Diversion', 'Diversion Shortage', 'Recoverable Loss'}.issubset(bud_df.columns):
                    col_names = []
                    for col in bud_df.columns:
                        if "Actual Delivery" in col:
                            col, dest = col.split("to")
                        elif "Delivery Shortage" in col:
                            col = col.split("for")[0]
                        col_names.append(col.strip())
                    
                    bud_df.columns = col_names
                    
                    if 'Element Group' in dest:
                        bud_df["Destination Type"] = 6
                        bud_df["Destination"] = int(dest.split()[-1])
                    elif 'Element' in dest:
                        bud_df["Destination Type"] = 2
                        bud_df["Destination"] = int(dest.split()[-1])
                    elif 'Subregion' in dest:
                        bud_df["Destination Type"] = 4
                        bud_df["Destination"] = int(dest.split()[-1])
                    else:
                        bud_df["Destination Type"] = 0
                        bud_df["Destination"] = 0

                # clean column names
                col_names_clean = []
                for c in bud_df.columns:
                    for v in [" ", "(+)", "(-)", "(=)"]:
                        c = c.replace(v, "")
                    col_names_clean.append(c)
                bud_df.columns = col_names_clean

                bud_df["Location"] = location
                bud_df["Scenario"] = scenario
    
                # write to postgres
                bud_df.to_sql(bud_tbl_name, con=engine, if_exists="append", index=False)


def run_bash(command, working_path):
    """
    Run Bash command or script from python
    """
    bash_path = shutil.which("bash")
    bash_command = [bash_path, "-c", command]
    print(f"Running bash command {bash_command}...")
    m = Popen(
        bash_command,
        stdout=PIPE,
        stderr=STDOUT,
        env={
            "LD_LIBRARY_PATH": "/opt/intel/oneapi/compiler/2022.0.2/linux/compiler/lib/intel64_lin"
        },
        cwd=working_path,
    )
    if m.stdout is not None:
        for raw_line in iter(m.stdout.readline, b""):
            line = raw_line.decode("utf-8", errors="backslashreplace").rstrip()
            print(line)

    m.wait()

    print(f"The command finished with return code: {m.returncode}")
    if m.returncode != 0:
        raise RuntimeError(f"'{bash_command}' did not run successfully.")


def backup_database(username, db_name):
    """ """
    # export postgres tables to .sql with pgdump
    sql_file = f"/data/{db_name}.sql"
    run_bash(
        f"pg_dump --host=127.0.0.1 --dbname={db_name} --username={username} --format=c --no-password > {sql_file}",
        working_path,
    )

    # upload .sql file to S3
    s3_upload2(sql_file)


def run_model(
    scenario, scenario_year, working_path, username, password, db_name, server
):
    """
    Run model

    Parameters
    ----------
    scenario : str
        Scenario ID for the model run

    scenario_year : int
        year corresponding to the scenario pumping

    working_path : str
        path where all files are located
    """
    scenario_path = os.path.join(working_path, "FilesToCopy")

    # retrieve scenario files from s3
    if scenario.upper() != "BASE":
        # set title for well specification file
        ws_title = f"{scenario_year}_WELLSPEC_{scenario}.DAT"

        # set title of pumping rates file for scenario
        pump_title = f"{scenario_year}_PUMPING_{scenario}.DAT"

        # make directory to store scenario files
        make_directory(scenario_path)

        print(f"Retrieving {ws_title} from S3")
        scenario_ws = os.path.join(scenario_path, ws_title)
        s3_download2(scenario_ws)

        print(f"Retrieving {pump_title} from S3")
        scenario_pumping = os.path.join(scenario_path, pump_title)
        s3_download2(scenario_pumping)

        if os.path.exists(scenario_ws) and os.path.exists(scenario_pumping):
            print(f"Copying {ws_title} to C2VSimFG_WellSpec.dat")
            shutil.copy(
                scenario_ws,
                os.path.join(
                    working_path, "Simulation/Groundwater/C2VSimFG_WellSpec.dat"
                ),
            )

            print(f"Copying {pump_title} to C2VSimFG_PumpRates.dat")
            shutil.copy(
                scenario_pumping,
                os.path.join(
                    working_path, "Simulation/Groundwater/C2VSimFG_PumpRates.dat"
                ),
            )

    # run bash script
    print("Running model...")
    run_bash("./run_model.sh", working_path)

    # create database engine
    engine = postgres_engine(username, password, db_name, server)

    # load hdf data to postgres
    results_dir = os.path.join(working_path, "Results")
    budgethdf2postgres(results_dir, engine, scenario)

    zip_name = os.path.join(working_path, f"Scenario{scenario}.zip")
    zip_model(
        zip_name, [os.path.join(working_path, p) for p in ["Preprocessor", "Simulation", "Results"]]
    )

    file_size, units = get_file_size(zip_name)
    print(f"{zip_name} created is {file_size:6.2f} {units}")

    s3_upload2(zip_name)


# input information
working_path = os.getenv("WORKING_PATH")
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

# database settings
username = "analyst"
password = "analyst"
db_name = "analysis"
server = "localhost"

# set path to GIS data
gis_dir = os.path.join(working_path, "GIS")
gis_ext = "zip"

# set full paths to projects file
projects_file = os.path.join(working_path, prj_file)

# read file containing list of projects
projects = pd.read_csv(projects_file)
scenario_ids = projects["Scenario"].tolist()

with DAG(
    dag_id="run_single_model",
    schedule=None,
    start_date=datetime.today(),
    catchup=False,
    dagrun_timeout=timedelta(days=1),
    tags=["iwfm"],
) as dag:

    check_info = PythonOperator(
        task_id="GetInfo",
        python_callable=print_info,
        execution_timeout=timedelta(minutes=1),
        dag=dag,
    )

    load_gis = PythonOperator(
        task_id="LoadGISData",
        python_callable=gis2postgis,
        op_kwargs={
            "gis_dir": gis_dir,
            "gis_ext": gis_ext,
            "username": username,
            "password": password,
            "db_name": db_name,
            "server": server,
        },
        execution_timeout=timedelta(hours=1),
        dag=dag,
    )

    backup_db = PythonOperator(
        task_id="BackupDatabase",
        python_callable=backup_database,
        op_kwargs={
            "username": username,
            "db_name": db_name,
        },
        execution_timeout=timedelta(hours=2),
        dag=dag,
    )

    run_base = PythonOperator(
        task_id=f"RunBaseline",
        python_callable=run_model,
        op_kwargs={
            "scenario": "base",
            "scenario_year": scenario_year,
            "working_path": working_path,
            "username": username,
            "password": password,
            "db_name": db_name,
            "server": server,
        },
        execution_timeout=timedelta(hours=12),
        dag=dag,
    )

    check_info >> load_gis >> run_base >> backup_db

if __name__ == "__main__":
    print("Testing DAG")
    dag.test()
