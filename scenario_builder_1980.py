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

from read_wellspec import IWFMWells
from read_pumprates import IWFMPumpRates
from generate_scenario_pumping import generate_pumping, generate_pumping_column_references
from write_pumprates import write_pumping_rates
from write_wellspec import write_well_specifications

matplotlib.use('Agg')

# input information
working_path = ""
gw_path = "Simulation/Groundwater"
ws_file = "C2VSimFG_WellSpec.dat"
pr_file = "C2VSimFG_PumpRates.dat"
wn_file = "well_names_and_owners.csv"
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

# read file containing list of projects
projects = pd.read_csv(transfer_projects_file)

transfer_projects = projects["Project"].tolist()

# set full path to well spec file and well names and owners
well_spec_file = os.path.join(gw_path, ws_file)
well_names_file = os.path.join(gw_path, wn_file)

# read well specification file
well_spec = IWFMWells.from_file(well_spec_file)
wells = well_spec.to_dataframe()

# read well names and owners
#well_names = pd.read_csv(wn_file)

# join well names and owners with well specification information
#wells = wells.join(well_names)

# read element groups from well specifications file
element_groups = well_spec.get_element_groups_as_list()

# read pump rates file
pumprates_file = os.path.join(gw_path, pr_file)
pump_rates = IWFMPumpRates.from_file(pumprates_file)

# generate column reference for each project well
wells = generate_pumping_column_references(
    transfer_projects,
    wells,
    well_names_file,
    "ICOLWL2",
    pump_rates.n_columns
)

# read the base case pump rates data
pump_rates_ts = pump_rates.to_dataframe()

# trim the dataset for the simulation start date
pump_rates_ts = (
    pump_rates_ts[pump_rates_ts["Date"] >= "1973-10-31"].copy().reset_index(drop=True)
)

# loop through each project to generate the pumping timeseries file
for proj_no, proj in enumerate(transfer_projects, start=1):

    print(f"Creating pumping time series data for project {proj_no} of {len(transfer_projects)}: {proj}")

    # get ID from 'Scenario' column in TransferProjects.csv
    scenario = projects[projects["Project"] == proj]["Scenario"].to_numpy()[0]

    # set title of pumping rates file for scenario
    pump_title = f"{scenario_year}_PUMPING_{scenario:02d}"
    
    new_columns = generate_pumping(
        proj,
        wells,
        pump_rates_ts,
        scenario_year,
        pumping_duration,
        scenario_folder,
        output_folder,
        qa_folder,
        pump_title
    )

    # merge the columns for the new well time series with the original pump rates time series
    proj_pump_rates = pd.merge(pump_rates_ts, new_columns, on="Date")

    # write pumping rates time series data file for project
    print(f"Writing pumping time series file for project {proj_no} of {len(transfer_projects)}: {proj}")
    write_pumping_rates(
        f"{scenario_folder}/{output_folder}/{pump_title}.DAT",
        proj_pump_rates,
        16
    )

    # set up project well specification data to generate input file
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

    # get list of column names for well properties and pumping configuration
    ws_col = well_spec.get_property_names() # TODO: may want to add name and owner here
    wc_col = well_spec.get_pump_config_names()

    # concatenate the original well location information with the project-specific wells (duplicates)
    update_ws = pd.concat(
        [
            wells[ws_col],
            proj_wells[ws_col],
        ],
        ignore_index=True,
    )

    # concatenate the original well pumping information with the project-specific wells (duplicates)
    update_wc = pd.concat(
        [
            wells[wc_col],
            proj_wells[wc_col],
        ],
        ignore_index=True,
    )

    # write the wellspec data file
    print(f"Writing well specification input file for project {proj_no} of {len(transfer_projects)}: {proj}")
    write_well_specifications(
        f"{scenario_folder}/{output_folder}/{ws_title}.DAT",
        update_ws,
        update_wc,
        element_groups
    )