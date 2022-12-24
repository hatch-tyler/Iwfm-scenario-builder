#!/usr/bin/env python
# coding: utf-8

# ## 1980 Streamflow Depletion Scenario
# ---
# Generates the time-series pumping file and well specifications file for each transfer project

import os
import matplotlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from generate_scenario_pumping import generate_pumping
from write_pumprates import write_pumping_rates
from write_wellspec import write_well_specifications

matplotlib.use('Agg')

# input information
gw_path = "Simulation/Groundwater"
ws_file = "C2VSimFG_WellSpec.dat"
pumprates_file = "C2VSimFG_PumpRates.dat"
well_names_file = "well_names_and_owners.csv"
transfer_projects_file = "TransferProjects.csv"
scenario_year = 1980
pumping_duration = 6
scenario_folder = f"{scenario_year}"
output_folder = "FilesToCopy"
qa_folder = "QA"

# create output folders
if not os.path.exists(scenario_folder):
    os.mkdir(scenario_folder)
    os.mkdir(os.path.join(scenario_folder, output_folder))
    os.mkdir(os.path.join(scenario_folder, output_folder, qa_folder))

elif not os.path.exists(os.path.join(scenario_folder, output_folder)):
    os.mkdir(os.path.join(scenario_folder, output_folder))
    os.mkdir(os.path.join(scenario_folder, output_folder, qa_folder))

elif not os.path.exists(os.path.join(scenario_folder, output_folder, qa_folder)):
    os.mkdir(os.path.join(scenario_folder, output_folder, qa_folder))

# get well specification information from file
ws_cols = "ID       XWELL     YWELL       RWELL      PERFT      PERFB"

ws_path = os.path.join(gw_path, ws_file)

if not os.path.exists(ws_path):
    print(f"{ws_file} does not exist")

ws = pd.read_csv(
    ws_path,
    header=None,
    names=ws_cols.split(),
    skiprows=94,
    nrows=610,
    comment="/",
    delim_whitespace=True,
)

# read well names and owners
well_names = pd.read_csv(os.path.join(gw_path, well_names_file))

# join well names and owners with well specification information
ws = ws.join(well_names)

# get well characteristics from file
wc_cols = "ID      ICOLWL   FRACWL    IOPTWL   TYPDSTWL    DSTWL   ICFIRIGWL   ICADJWL  ICWLMAX   FWLMAX"

wc = pd.read_csv(
    os.path.join(gw_path, ws_file),
    header=None,
    names=wc_cols.split(),
    skiprows=753,
    nrows=610,
    delim_whitespace=True,
)

# combine well specifications and well characteristics using the well ID column
wells = pd.merge(ws, wc, on="ID")

# read element groups from well specifications file
element_groups = []
with open(os.path.join(gw_path, ws_file), "r") as f:
    for i, line in enumerate(f):
        if i >= 1381:
            element_groups.append(line)

# read file containing list of projects
projects = pd.read_csv(transfer_projects_file)

transfer_projects = projects["Project"].tolist()

# generate column reference for each project well
wells["ICOLWL2"] = 0
current_col = 66311 # this is ncolpump from the pump rates data file + 1
for proj in transfer_projects:
    proj_wells = wells[wells["Owner"] == proj].sort_values(by="ICOLWL")
    well_ids = proj_wells["ICOLWL"].tolist()
    for wid in well_ids:
        wells.loc[
            (wells["Owner"] == proj) & (wells["ICOLWL"] == wid), "ICOLWL2"
        ] = current_col
        current_col += 1

wells.sort_values(by="ICOLWL2", inplace=True)

# read the base case pump rates file
pump_rates = pd.read_csv(
    os.path.join(gw_path, pumprates_file),
    header=None,
    skiprows=5,
    delim_whitespace=True,
)

# convert the Date column to datetime object
pump_rates.rename(columns={0: "Date"}, inplace=True)
pump_rates["Date"] = pump_rates["Date"].apply(lambda d: d.split("_")[0])
pump_rates["Date"] = pd.to_datetime(pump_rates["Date"], format="%m/%d/%Y")

# trim the dataset for the simulation start date
pump_rates = (
    pump_rates[pump_rates["Date"] >= "1973-10-31"].copy().reset_index(drop=True)
)

# loop through each project to generate the pumping timeseries file
for proj_no, proj in enumerate(transfer_projects, start=1):

    print(f"Creating pumping time series data for project {proj_no} of {len(transfer_projects)}: {proj}")

    # get ID from 'Scenario' column in TransferProjects.csv
    scenario = projects[projects["Project"] == proj]["Scenario"].to_numpy()[0]

    # set title of pumping rates file for scenario
    title = f"{scenario_year}_PUMPING_{scenario:02d}"
    
    new_columns = generate_pumping(
        proj,
        wells,
        pump_rates,
        scenario_year,
        pumping_duration,
        scenario_folder,
        output_folder,
        qa_folder,
        title
    )

    # merge the columns for the new well time series with the original pump rates time series
    proj_pump_rates = pd.merge(pump_rates, new_columns, on="Date")

    # write pumping rates time series data file for project
    print(f"Writing pumping time series file for project {proj_no} of {len(transfer_projects)}: {proj}")
    write_pumping_rates(
        f"{scenario_folder}/{output_folder}/{title}.DAT",
        proj_pump_rates,
        16
    )

    # set up project well specification data to generate input file
    print(f"Writing well specification input file for project {proj_no} of {len(transfer_projects)}: {proj}")
    
    # set title for well specification file
    ws_title = f"{scenario_year}_WELLSPEC_{scenario:02d}"

    # create a copy of the wells for the project
    proj_wells = wells[wells["Owner"] == proj].copy().reset_index(drop=True)
    
    # generate new IDs for the duplicated wells
    proj_wells["ID"] = np.arange(len(wells) + 1, len(wells) + len(proj_wells) + 1)

    # fill in values for other columns
    proj_wells["ICOLWL"] = proj_wells["ICOLWL2"]
    proj_wells["ICWLMAX"] = proj_wells["ICOLWL2"]
    proj_wells["TYPDSTWL"] = 0
    proj_wells["DSTWL"] = 0
    proj_wells["ICFIRIGWL"] = 0

    # concatenate the original well location information with the project-specific wells (duplicates)
    update_ws = pd.concat(
        [
            ws,
            proj_wells[
                ["ID", "XWELL", "YWELL", "RWELL", "PERFT", "PERFB", "Name", "Owner"]
            ],
        ],
        ignore_index=True,
    )

    # concatenate the original well pumping information with the project-specific wells (duplicates)
    update_wc = pd.concat(
        [
            wc,
            proj_wells[
                [
                    "ID",
                    "ICOLWL",
                    "FRACWL",
                    "IOPTWL",
                    "TYPDSTWL",
                    "DSTWL",
                    "ICFIRIGWL",
                    "ICADJWL",
                    "ICWLMAX",
                    "FWLMAX",
                ]
            ],
        ],
        ignore_index=True,
    )

    # write the wellspec data file
    write_well_specifications(
        f"{scenario_folder}/{output_folder}/{ws_title}.DAT",
        update_ws,
        update_wc,
        element_groups
    )