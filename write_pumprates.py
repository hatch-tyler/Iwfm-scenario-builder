import os
import numpy as np
import pandas as pd

def format_date(date_col):
    return date_col.strftime("%m/%d/%Y_24:00")

def write_pumping_rates(
    out_name: str,
    pump_rates_df: pd.DataFrame,
    chunk_size : int,
):
    """
    Write IWFM pump rates time series data file

    Parameters
    ----------
    out_name : str
        out path and name of pump rates time series data file
    
    pump_rates_df: pd.DataFrame
        pandas DataFrame containing pumping time series data needed for IWFM

    chunk_size : int
        number of time series data rows written at a time to output file

        .. note:: Uses pd.DataFrame.to_string() for formatting. this can cause
                  memory issues in some cases.
    """
    # generates chunks to write pump rates file to not run out of memory
    indices = np.arange(0, len(pump_rates_df), chunk_size)

    if indices[-1] != len(pump_rates_df):
        indices = np.append(indices, len(pump_rates_df))

    # write the pumping rates data file
    with open(out_name, "w") as f:
        f.write(
            " " * 4 + "{:<49d}".format(pump_rates_df.shape[-1] - 1) + "/ NCOLPUMP\n"
        )
        f.write(" " * 4 + "{:<49.1f}".format(43560.0) + "/ FACTPUMP\n")
        f.write(" " * 4 + "{:<49d}".format(1) + "/ NSPPUMP\n")
        f.write(" " * 4 + "{:<49d}".format(0) + "/ NFQPUMP\n")
        f.write(" " * 4 + "{:<49s}".format("") + "/ DSSFL\n")
        #pump_rates_df.to_string(
        #    f,
        #    header=False,
        #    index=False,
        #    formatters={"Date": format_date},
        #    float_format="%9.3f",
        #)   
        for i, val in enumerate(indices[:-1], start=1):
            print(f"Writing chunk: {i}")
            rows = pump_rates_df.iloc[indices[i-1]:indices[i]]
            rows.to_string(
                f,
                header=False,
                index=False,
                formatters={"Date": format_date},
                float_format="%9.3f",
            )