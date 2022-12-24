import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def generate_pumping(
    proj: str, 
    wells: pd.DataFrame, 
    pump_rates: pd.DataFrame, 
    scenario_year: int, 
    pumping_duration: int,
    scenario_folder: str,
    output_folder: str,
    qa_folder: str,
    title: str
) -> pd.DataFrame:
    """
    Generates new pumping time series for the wells in the project and scenario.

    Parameters
    ----------
    proj : str
        project name

    wells : pd.DataFrame
        pandas DataFrame containing well specification information

    pump_rates: pd.DataFrame
        pandas DataFrame containing time series pump rates

    scenario_year : int
        year when scenario pumping is added

    pumping_duration : int
        max number of time series steps to include

    scenario_folder : str
        folder name for project scenario

    output_folder : str
        folder name for file output

    qa_folder : str
        folder name for QA plots

    title : str
        name of pumping scenario file

    Returns
    -------
    pd.DataFrame
        pandas DataFrame containing new columns for the project scenario wells
    """
    # create a list of pumping column references for the project wells
    proj_wells = wells[wells["Owner"] == proj]["ICOLWL"].tolist()
    
    # create a new dataframe (copy not a slice) of the pumping time series for the project wells
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

        plot_pumping(
            proj,
            well_name,
            df,
            col,
            new_columns,
            nc_column,
            scenario_year,
            f"{scenario_folder}/{output_folder}/{qa_folder}/{title}_{well_name}.png"
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
    out_name: str
):
    """
    Plot Scenario Pumping with original pumping for QA
    """
    # plot for QA
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(scenario_pump_df["Date"].to_numpy(), scenario_pump_df[scenario_pump_col].to_numpy(), label=f"{scenario_year}")
    ax.plot(orig_pump_df["Date"].to_numpy(), orig_pump_df[orig_pump_col].to_numpy(), label="Actual")
    ax.legend()
    ax.set_xlabel("Time")
    ax.set_ylabel("Volume (AF)")
    ax.set_title(f"{scenario_year} Pumping for {proj}\nWell: {well_name}")
    plt.savefig(out_name)
    plt.close()
    print(f"Plot for {proj}: {well_name} saved")
