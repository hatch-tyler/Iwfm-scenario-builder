#!/usr/bin/env python
# coding: utf-8

# ## 1980 Streamflow Depletion Scenario
# ---
# Generates the time-series pumping file and well specifications file for each transfer project

import os
import matplotlib as mpl
import pandas as pd

from utilities import make_directory
from read_wellspec import IWFMWells
from read_pumprates import IWFMPumpRates
from generate_scenario_pumping import (
    generate_project_pumping_scenario,
)

mpl.use("Agg")


def scenario_builder(
    project: str,
    working_path: str,
    gw_path: str,
    ws_file: str,
    pr_file: str,
    wn_file: str,
    start_date: str,
    scenario_year: int,
    pumping_duration: int,
    output_path: str,
    qa_path: str,
):
    """
    Build pumping scenario for a project

    Parameters
    ----------
    project : str
        name of project with one or more wells

    working_path : str
        path to working directory

    gw_path : str
        relative path to groundwater directory

    ws_file : str
        well specification data file name

    pr_file : str
        pump rates data file name

    wn_file :str
        name of file containing well name and project

    start_date : str
        model start date

    scenario_year : int
        model year when scenario pumping occurs

    pumping_duration : int
        number of model time steps corresponding to scenario pumping

    output_path : str
        relative path to where scenario files are written

    qa_path : str
        directory relative to output path where QA plots are written

    Returns
    -------
    None
        writes well spec and pump rates file for scenario
    """
    # set full paths to input files
    full_gw_path = os.path.join(working_path, gw_path)
    well_spec_file = os.path.join(full_gw_path, ws_file)
    well_names_file = os.path.join(full_gw_path, wn_file)
    pumprates_file = os.path.join(full_gw_path, pr_file)

    # set output paths
    scenario_path = str(scenario_year)
    output_dir = os.path.join(working_path, scenario_path, output_path)
    qa_dir = os.path.join(output_dir, qa_path)

    # create output folders
    make_directory(qa_dir)

    # read well specification file
    well_spec = IWFMWells.from_file(well_spec_file)
    wells = well_spec.to_dataframe()

    # read well names and owners
    well_names = pd.read_csv(well_names_file)

    # join well names and owners with well specification information
    wells = wells.join(well_names)

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

    # generate project well specification file
    generate_project_pumping_scenario(
        project,
        projects,
        scenario_year,
        pumping_duration,
        pump_rates.n_columns,
        wells,
        ws_col,
        wc_col,
        element_groups,
        pump_rates_ts,
        output_dir,
        qa_dir,
    )


if __name__ == "__main__":

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
    output_path = "FilesToCopy"
    qa_path = "QA"

    # set full paths to projects file
    projects_file = os.path.join(working_path, prj_file)

    # read file containing list of projects
    projects = pd.read_csv(projects_file)

    transfer_projects = projects["Project"].tolist()

    # loop through each project to generate the pumping timeseries file
    for proj in transfer_projects:

        scenario_builder(
            proj,
            working_path,
            gw_path,
            ws_file,
            pr_file,
            wn_file,
            start_date,
            scenario_year,
            pumping_duration,
            output_path,
            qa_path,
        )
