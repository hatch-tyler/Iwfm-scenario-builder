import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from scenario_builder.write_pumprates import write_pumping_rates
from scenario_builder.write_wellspec import write_well_specifications
from scenario_builder.utilities import s3_upload


def generate_project_pumping_scenario(
    scenario: int,
    projects: pd.DataFrame,
    scenario_year: int,
    pumping_duration: int,
    n_columns: int,
    wells: pd.DataFrame,
    ws_col: list,
    wc_col: list,
    element_groups: list,
    pump_rates_ts: pd.DataFrame,
    output_dir: str,
    qa_dir: str,
):
    """
    Create and write well specification file and pump rates file

    Parameters
    ----------
    scenario : int
        project ID

    projects : pd.DataFrame
        pandas DataFrame containing project information

    scenario_year : int
        year corresponding to project pumping

    n_columns : int
        number of columns in the pump rates data file

    wells : pd.DataFrame
        pandas DataFrame containing well specification information

    ws_col : list
        well specification column names for data block in input file

    wc_col : list
        well pumping column names for data block in input file

    element_groups : list
        element group data to write to well specification file

    pump_rates_ts : pd.DataFrame
        pandas DataFrame containing pump rate time series

    output_dir : str
        output path to project pump rates file

    qa_dir : str
        output path to project pump rates qa plot

    Returns
    -------
    None
        writes project data to new well specification file
    """
    # get project name from scenario ID
    proj = projects[projects["Scenario"] == scenario]["Project"].tolist()[0]

    print(f"Project {scenario} of {len(projects)}: {proj}")

    # set title for well specification file
    ws_title = f"{scenario_year}_WELLSPEC_{scenario:02d}"

    # set title of pumping rates file for scenario
    pump_title = f"{scenario_year}_PUMPING_{scenario:02d}"

    # select the wells for the project
    proj_wells = generate_pumping_column_references(proj, wells, "ICOLWL2", n_columns)

    # generate scenario pumping time series for project
    new_columns = generate_pumping(
        proj,
        proj_wells,
        pump_rates_ts,
        scenario_year,
        pumping_duration,
        qa_dir,
        pump_title,
    )

    # create the scenario wells for the project
    scenario_wells = generate_scenario_wells(proj_wells, len(wells))

    # concatenate the original well location information with the project-specific wells (duplicates)
    update_ws = pd.concat(
        [
            wells[ws_col],
            scenario_wells[ws_col],
        ],
        ignore_index=True,
    )

    # concatenate the original well pumping information with the project-specific wells (duplicates)
    update_wc = pd.concat(
        [
            wells[wc_col],
            scenario_wells[wc_col],
        ],
        ignore_index=True,
    )

    # write the wellspec data file
    print("Writing well specification input file for project.")

    out_wells_file = os.path.join(output_dir, f"{ws_title}.DAT")
    write_well_specifications(out_wells_file, update_ws, update_wc, element_groups)

    # merge the columns for the new well time series with the original pump rates time series
    proj_pump_rates = pd.merge(pump_rates_ts, new_columns, on="Date")

    # write pumping rates time series data file for project
    print("Writing pumping time series file for project.")

    out_pumping_file = os.path.join(output_dir, f"{pump_title}.DAT")
    write_pumping_rates(out_pumping_file, proj_pump_rates, 16)

    print(f"Uploading {out_wells_file} to S3")
    s3_upload(out_wells_file)

    print(f"Uploading {out_pumping_file} to S3")
    s3_upload(out_pumping_file)


def generate_pumping_column_references(
    project: str,
    well_data: pd.DataFrame,
    col_name: str,
    n_columns: int,
) -> pd.DataFrame:
    """
    Generates column references for each project well in a project

    Parameters
    ----------
    project : str
        name of project with one or more wells

    well_data : pd.DataFrame
        pandas DataFrame of well specification data

    col_name : str
        column name for the generated column references

    n_columns : int
        number of columns in the pump rates data file

    Returns
    -------
    pd.DataFrame
        pandas DataFrame with column references for each project
    """
    # select project wells
    proj_wells = well_data[well_data["Owner"] == project].copy().reset_index(drop=True)
    proj_wells.sort_values(by="ICOLWL", inplace=True)

    # generate new column to hold scenario pumping column references
    proj_wells[col_name] = 0

    # start at the next column after the base pumping file
    current_col = n_columns + 1

    well_ids = proj_wells["ICOLWL"].tolist()

    # loop through each well in the project
    for wid in well_ids:
        proj_wells.loc[proj_wells["ICOLWL"] == wid, col_name] = current_col
        current_col += 1

    return proj_wells


def generate_scenario_wells(proj_wells: pd.DataFrame, n_wells: int) -> pd.DataFrame:
    """
    Generate new wells for project with scenario pumping

    Parameters
    ----------
    proj : pd.DataFrame
        pandas DataFrame containing well specifications for a project

    n_wells : int
        number of total wells in base model

    Returns
    -------
    pd.DataFrame
        pandas DataFrame containing project well with scenario information
    """
    # generate new IDs for the duplicated wells
    proj_wells["ID"] = np.arange(n_wells + 1, n_wells + len(proj_wells) + 1)

    # fill in values for other columns
    proj_wells["ICOLWL"] = proj_wells["ICOLWL2"]
    proj_wells["ICWLMAX"] = proj_wells["ICOLWL2"]
    proj_wells["TYPDSTWL"] = 0
    proj_wells["DSTWL"] = 0
    proj_wells["ICFIRIGWL"] = 0

    return proj_wells


def generate_pumping(
    proj: str,
    proj_wells: pd.DataFrame,
    pump_rates: pd.DataFrame,
    scenario_year: int,
    pumping_duration: int,
    plot_path: str,
    title: str,
) -> pd.DataFrame:
    """
    Generate new pumping time series for the wells in the project and scenario.

    Parameters
    ----------
    proj : str
        project name

    proj_wells : pd.DataFrame
        pandas DataFrame containing well specification information for project

    pump_rates: pd.DataFrame
        pandas DataFrame containing time series pump rates

    scenario_year : int
        year when scenario pumping is added

    pumping_duration : int
        max number of time steps to include

    plot_path : str
        path to save QA plot

    title : str
        pumping scenario name

    Returns
    -------
    pd.DataFrame
        pandas DataFrame containing new columns for the project scenario wells
    """
    # get project wells. these need to be sorted by new column reference so they are written in order.
    proj_wells = proj_wells.sort_values(by="ICOLWL2")

    # create a list of pumping column references for the project wells
    proj_pumping_cols = proj_wells["ICOLWL"].tolist()

    # create a new dataframe (copy not a slice) of the pumping time series for the project wells
    df = pump_rates[["Date"] + proj_pumping_cols].copy()

    # get column names for wells in transfer project
    df_columns = [col for col in df.columns if col != "Date"]

    # sum pumping values for all wells in project to determine first date of pumping
    df["total"] = df[df_columns].sum(axis=1)

    # determine index of first pumping in any of the wells in transfer project
    first_index = df["total"].ne(0).idxmax()
    max_index = df.index.max()

    # number of months pumping is up to the pumping_duration
    months_pumping = (
        (max_index - first_index + 1)
        if (first_index + pumping_duration > max_index)
        else pumping_duration
    )

    last_index = first_index + months_pumping

    # get start month and day for first pumping
    month = df.iloc[first_index]["Date"].month
    day = df.iloc[first_index]["Date"].day
    hour = df.iloc[first_index]["Date"].hour
    minute = df.iloc[first_index]["Date"].minute

    # create template dataframe for new time series
    new_columns = pd.DataFrame(
        data=0,
        index=pump_rates["Date"].to_numpy(),
        columns=proj_wells["ICOLWL2"].to_numpy(),
    )
    new_columns.reset_index(inplace=True)
    new_columns.rename(columns={"index": "Date"}, inplace=True)

    # loop over the time series for each well in the project to generate new time series
    for col in df_columns:
        well_name = proj_wells[proj_wells["ICOLWL"] == col]["Name"].to_numpy()[0]
        nc_column = proj_wells[proj_wells["ICOLWL"] == col]["ICOLWL2"].to_numpy()[0]

        start_date = f"{scenario_year}-{month}-{day} {hour}:{minute}"

        # get indices for duration of pumping data
        start_index = new_columns[new_columns["Date"] == start_date].index[0]
        end_index = start_index + months_pumping

        pumping = df.loc[
            (df.index >= first_index) & (df.index < last_index), col
        ].to_numpy()

        # set first months of transfer pumping to same dates in scenario_year
        new_columns.loc[
            (new_columns.index >= start_index) & (new_columns.index < end_index),
            nc_column,
        ] = pumping

        # set the name of the qa plot
        out_name = os.path.join(plot_path, f"{title}_{well_name}.png")

        # plot time series for visual QA
        plot_pumping(
            proj, well_name, df, col, new_columns, nc_column, scenario_year, out_name
        )

    return new_columns


def plot_pumping(
    proj: str,
    well_name: str,
    orig_pump_df: pd.DataFrame,
    orig_pump_col: int,
    scenario_pump_df: pd.DataFrame,
    scenario_pump_col: int,
    scenario_year: int,
    out_name: str,
):
    """
    Plot Scenario Pumping with original pumping for QA
    """
    # plot for QA
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(
        scenario_pump_df["Date"].to_numpy(),
        scenario_pump_df[scenario_pump_col].to_numpy(),
        label=f"{scenario_year}",
    )
    ax.plot(
        orig_pump_df["Date"].to_numpy(),
        orig_pump_df[orig_pump_col].to_numpy(),
        label="Actual",
    )
    ax.legend()
    ax.set_xlabel("Time")
    ax.set_ylabel("Volume (AF)")
    ax.set_title(f"{scenario_year} Pumping for {proj}\nWell: {well_name}")
    plt.savefig(out_name)
    plt.close()
    print(f"Plot for {proj} well: {well_name} saved")
