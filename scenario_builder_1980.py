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

from utilities import make_directory
from read_wellspec import IWFMWells
from read_pumprates import IWFMPumpRates
from generate_scenario_pumping import (
    generate_pumping,
    generate_pumping_column_references,
    generate_wells,
)
from write_pumprates import write_pumping_rates
from write_wellspec import write_well_specifications

matplotlib.use("Agg")

# input information
working_path = os.getcwd()
gw_path = "Simulation/Groundwater"
ws_file = "C2VSimFG_WellSpec.dat"
pr_file = "C2VSimFG_PumpRates.dat"
wn_file = "well_names_and_owners.csv"
prj_file = "TransferProjects.csv"
start_date = "1973-10-31"
scenario_year = 1980
pumping_duration = 6
scenario_folder = f"{scenario_year}"
output_folder = "FilesToCopy"
qa_folder = "QA"

# create output folders
make_directory(os.path.join(scenario_folder, output_folder, qa_folder))

# set full paths to input files
full_gw_path = os.path.join(working_path, gw_path)
projects_file = os.path.join(working_path, prj_file)
well_spec_file = os.path.join(full_gw_path, ws_file)
well_names_file = os.path.join(full_gw_path, wn_file)
pumprates_file = os.path.join(full_gw_path, pr_file)

# set output paths
output_dir = os.path.join(working_path, scenario_folder, output_folder)
qa_dir = os.path.join(output_dir, qa_folder)

# read file containing list of projects
projects = pd.read_csv(projects_file)

transfer_projects = projects["Project"].tolist()

# read well specification file
well_spec = IWFMWells.from_file(well_spec_file)
wells = well_spec.to_dataframe()

# get list of column names for well properties and pumping configuration
ws_col = well_spec.get_property_names()  # TODO: may want to add name and owner here
wc_col = well_spec.get_pump_config_names()

# read element groups from well specifications file
element_groups = well_spec.get_element_groups_as_list()

# read pump rates file
pump_rates = IWFMPumpRates.from_file(pumprates_file)

# read the base case pump rates data
pump_rates_ts = pump_rates.to_dataframe()

# trim the dataset for the simulation start date
pump_rates_ts = (
    pump_rates_ts[pump_rates_ts["Date"] >= start_date].copy().reset_index(drop=True)
)

# generate column reference for each project well
wells = generate_pumping_column_references(
    transfer_projects, wells, well_names_file, "ICOLWL2", pump_rates.n_columns
)

# loop through each project to generate the pumping timeseries file
for proj_no, proj in enumerate(transfer_projects, start=1):

    print(
        f"Creating pumping time series data for project {proj_no} of {len(transfer_projects)}: {proj}"
    )

    # get ID from 'Scenario' column in TransferProjects.csv
    scenario = projects[projects["Project"] == proj]["Scenario"].to_numpy()[0]

    # set title of pumping rates file for scenario
    pump_title = f"{scenario_year}_PUMPING_{scenario:02d}"

    new_columns = generate_pumping(
        proj, wells, pump_rates_ts, scenario_year, pumping_duration, qa_dir, pump_title
    )

    # merge the columns for the new well time series with the original pump rates time series
    proj_pump_rates = pd.merge(pump_rates_ts, new_columns, on="Date")

    # write pumping rates time series data file for project
    print(
        f"Writing pumping time series file for project {proj_no} of {len(transfer_projects)}: {proj}"
    )
    out_pumping_file = os.path.join(output_dir, f"{pump_title}.DAT")
    write_pumping_rates(out_pumping_file, proj_pump_rates, 16)

    # set up project well specification data to generate input file
    # set title for well specification file
    ws_title = f"{scenario_year}_WELLSPEC_{scenario:02d}"

    # create the wells for the project
    proj_wells = generate_wells(proj, wells)

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
    print(
        f"Writing well specification input file for project {proj_no} of {len(transfer_projects)}: {proj}"
    )
    out_wells_file = os.path.join(output_dir, f"{ws_title}.DAT")
    write_well_specifications(out_wells_file, update_ws, update_wc, element_groups)
