import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def generate_pumping_column_references(
    projects: list,
    well_data: pd.DataFrame,
    well_names_file: str,
    col_name: str,
    n_columns: int,
) -> pd.DataFrame:
    """
    Generates column references for each project well

    Parameters
    ----------
    projects : list
        list of projects to group wells

    well_data : pd.DataFrame
        pandas DataFrame of well specification data

    well_names_file : str
        file containing linkages between wells in well specifications and projects

    col_name : str
        column name for the generated column references

    n_columns : int
        number of columns in the pump rates data file

    Returns
    -------
    pd.DataFrame
        pandas DataFrame with column references for each project
    """
    # read well names and owners
    well_names = pd.read_csv(well_names_file)

    # join well names and owners with well specification information
    well_data = well_data.join(well_names)

    # generate new column to hold scenario pumping column references
    well_data[col_name] = 0

    # start at the next column after the base pumping file
    current_col = n_columns + 1

    # loop through each of the projects
    for proj in projects:
        proj_wells = well_data[well_data["Owner"] == proj].sort_values(by="ICOLWL")
        well_ids = proj_wells["ICOLWL"].tolist()

        for wid in well_ids:
            well_data.loc[
                (well_data["Owner"] == proj) & (well_data["ICOLWL"] == wid), col_name
            ] = current_col
            current_col += 1

    return well_data


def generate_wells(project: str, well_data: pd.DataFrame) -> pd.DataFrame:
    """
    Generate new wells for project with scenario pumping

    Parameters
    ----------
    project : str
        project name or owner with wells

    well_data : pd.DataFrame
        pandas DataFrame containing well specifications

    Returns
    -------
    pd.DataFrame
        pandas DataFrame containing project well with scenario information
    """
    # create a copy of the wells for the project
    proj_wells = well_data[well_data["Owner"] == project].copy().reset_index(drop=True)

    # generate new IDs for the duplicated wells
    proj_wells["ID"] = np.arange(
        len(well_data) + 1, len(well_data) + len(proj_wells) + 1
    )

    # fill in values for other columns
    proj_wells["ICOLWL"] = proj_wells["ICOLWL2"]
    proj_wells["ICWLMAX"] = proj_wells["ICOLWL2"]
    proj_wells["TYPDSTWL"] = 0
    proj_wells["DSTWL"] = 0
    proj_wells["ICFIRIGWL"] = 0

    return proj_wells


def generate_pumping(
    proj: str,
    well_data: pd.DataFrame,
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

    well_data : pd.DataFrame
        pandas DataFrame containing well specification information

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
    proj_wells = well_data[well_data["Owner"] == proj].sort_values(by="ICOLWL2")

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
        columns=well_data["ICOLWL2"].to_numpy(),
    )
    new_columns.reset_index(inplace=True)
    new_columns.rename(columns={"index": "Date"}, inplace=True)

    # loop over the time series for each well in the project to generate new time series
    for col in df_columns:
        well_name = well_data[well_data["ICOLWL"] == col]["Name"].to_numpy()[0]
        nc_column = well_data[well_data["ICOLWL"] == col]["ICOLWL2"].to_numpy()[0]

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
    print(f"Plot for {proj}: {well_name} saved")
