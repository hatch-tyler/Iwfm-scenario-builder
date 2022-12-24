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

def format_date(date_col):
    return date_col.strftime("%m/%d/%Y_24:00")

# loop through each project to generate the pumping timeseries file
for proj_no, proj in enumerate(transfer_projects, start=1):

    print(f"Writing pumping time series file for project {proj_no} of {len(transfer_projects)}: {proj}")

    scenario = projects[projects["Project"] == proj]["Scenario"].to_numpy()[0]
    title = f"{scenario_year}_PUMPING_{scenario:02d}"
    proj_wells = wells[wells["Owner"] == proj]["ICOLWL"].tolist()
    df = pump_rates[["Date"] + proj_wells].copy()

    # get column names for wells in transfer project
    df_columns = [col for col in df.columns if col != "Date"]

    # sum pumping values for all wells in project to determine first date of pumping
    df["total"] = df[df_columns].sum(axis=1)

    # determine index of first pumping in any of the wells in transfer project
    first_index = df["total"].ne(0).idxmax()
    max_index = df.index.max()

    # number of months pumping is up to the pumping_duration
    months_pumping = (
        (max_index - first_index + 1) if (first_index + pumping_duration > max_index) else pumping_duration
    )

    last_index = first_index + months_pumping

    # get start month and day for first pumping
    month = df.iloc[first_index]["Date"].month
    day = df.iloc[first_index]["Date"].day

    # create template dataframe for new time series
    new_columns = pd.DataFrame(
        data=0, index=pump_rates["Date"].to_numpy(), columns=wells["ICOLWL2"].to_numpy()
    )
    new_columns.reset_index(inplace=True)
    new_columns.rename(columns={"index": "Date"}, inplace=True)

    # loop over the time series for each well in the project to generate new time series
    for col in df_columns:
        well_name = wells[wells["ICOLWL"] == col]["Name"].to_numpy()[0]
        nc_column = wells[wells["ICOLWL"] == col]["ICOLWL2"].to_numpy()[0]

        start_date = f"{scenario_year}-{month}-{day}"

        # get indices for 6 months of data
        start_index = new_columns[new_columns["Date"] == start_date].index[0]
        end_index = start_index + months_pumping

        pumping = df.loc[
            (df.index >= first_index) & (df.index < last_index), col
        ].to_numpy()

        # set first six months of transfer pumping to same dates in scenario_year
        new_columns.loc[
            (new_columns.index >= start_index) & (new_columns.index < end_index),
            nc_column,
        ] = pumping

        # plot for QA
        fig, ax = plt.subplots(figsize=(11, 6))
        ax.plot(new_columns["Date"].to_numpy(), new_columns[nc_column].to_numpy(), label=f"{scenario_year}")
        ax.plot(df["Date"].to_numpy(), df[col].to_numpy(), label="Actual")
        ax.legend()
        ax.set_xlabel("Time")
        ax.set_ylabel("Volume (AF)")
        ax.set_title(f"{scenario_year} Pumping for {proj}\nWell: {well_name}")
        plt.savefig(
            f"{scenario_folder}/{output_folder}/{qa_folder}/{title}_{well_name}.png"
        )
        plt.close()
        print(f"Plot for {well_name} saved")

    # merge the columns for the new well time series with the original pump rates time series
    proj_pump_rates = pd.merge(pump_rates, new_columns, on="Date")

    # generates chunks to write pump rates file to not run out of memory
    indices = np.arange(0, len(proj_pump_rates), 16)

    if indices[-1] != len(proj_pump_rates):
        indices = np.append(indices, len(proj_pump_rates))

    # write the pumping rates data file
    with open(f"{scenario_folder}/{output_folder}/{title}.DAT", "w") as f:
        f.write(
            " " * 4 + "{:<49d}".format(proj_pump_rates.shape[-1] - 1) + "/ NCOLPUMP\n"
        )
        f.write(" " * 4 + "{:<49.1f}".format(43560.0) + "/ FACTPUMP\n")
        f.write(" " * 4 + "{:<49d}".format(1) + "/ NSPPUMP\n")
        f.write(" " * 4 + "{:<49d}".format(0) + "/ NFQPUMP\n")
        f.write(" " * 4 + "{:<49s}".format("") + "/ DSSFL\n")
        #proj_pump_rates.to_string(
        #    f,
        #    header=False,
        #    index=False,
        #    formatters={"Date": format_date},
        #    float_format="%9.3f",
        #)   
        for i, val in enumerate(indices[:-1], start=1):
            print(f"Writing chunk: {i}")
            rows = proj_pump_rates.iloc[indices[i-1]:indices[i]]
            rows.to_string(
                f,
                header=False,
                index=False,
                formatters={"Date": format_date},
                float_format="%9.3f",
            )

    # write well specification input file
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