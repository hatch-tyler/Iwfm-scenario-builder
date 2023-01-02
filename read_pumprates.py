import os
import numpy as np
import pandas as pd

from typing import List
from datetime import datetime

from scenario_builder.utilities import read_data


class TimeStamp:
    def __init__(self, dt: str):
        d, t = dt.split("_")

        if t.strip() == "24:00":
            trev = "23:59"
            dt = f"{d}_{trev}"

        self.date = datetime.strptime(dt, "%m/%d/%Y_%H:%M")

    def __repr__(self):
        return f"TimeStamp(dt={self.date})"

    def to_iwfm_format(self):
        hours = self.date.hour
        minutes = self.date.minute

        if hours == 23 and minutes == 59:
            return datetime.strftime(self.date, "%m/%d/%Y_24:00")
        else:
            return datetime.strftime(self.date, "%m/%d/%Y_%H:%M")


class TimeStep:
    def __init__(self, timestamp: TimeStamp, values: List[float]):
        self.timestamp = timestamp
        self.values = values

    def __repr__(self):
        return f"TimeStep(timestamp={self.timestamp}, values=[{self.values[0]},{self.values[1]},...,{self.values[-1]}]"

    def to_list(self) -> list:
        return [self.timestamp.date] + self.values

    @classmethod
    def from_list(cls, string_list):
        values = []
        for i, val in enumerate(string_list):
            if i == 0:
                timestamp = TimeStamp(val)
            else:
                values.append(float(val))

        return cls(timestamp, values)


class IWFMPumpRates:
    def __init__(
        self,
        n_columns: int,
        fact_pumping: float,
        num_timesteps: int,
        repetition_frequency: int,
        dss_file: str,
        timeseries: List[TimeStep],
    ):
        self.n_columns = n_columns
        self.fact_pumping = fact_pumping
        self.num_timesteps = num_timesteps
        self.repetition_frequency = repetition_frequency
        self.dss_file = dss_file
        self.timeseries = timeseries

    def to_dataframe(self) -> pd.DataFrame:
        column_names = ["Date"] + [n for n in range(1, self.n_columns + 1)]
        data = [step.to_list() for step in self.timeseries]
        return pd.DataFrame(data, columns=column_names)

    @classmethod
    def from_file(cls, file_name):
        with open(file_name, "r") as f:
            n_columns = int(read_data(f))
            fact_pumping = float(read_data(f))
            num_timesteps = int(read_data(f))
            repetition_frequency = int(read_data(f))
            dss_file = read_data(f)

            timeseries = []
            line = read_data(f)
            while line:
                line_list = line.split()
                timeseries.append(TimeStep.from_list(line_list))

                line = read_data(f)

        return cls(
            n_columns,
            fact_pumping,
            num_timesteps,
            repetition_frequency,
            dss_file,
            timeseries,
        )


if __name__ == "__main__":
    pump_rates_file = "Simulation/Groundwater/C2VSimFG_PumpRates.dat"
    pump_rates = IWFMPumpRates.from_file(pump_rates_file)

    print(pump_rates.n_columns)
    print(len(pump_rates.timeseries))
    df = pump_rates.to_dataframe()
    # print(df.info())
    print(df[df["Date"] == "1980-10-31 23:59"])
    start_index = 33
    print(df.iloc[start_index]["Date"].year)
    print(df.iloc[start_index]["Date"].month)
    print(df.iloc[start_index]["Date"].hour)
    print(df.iloc[start_index]["Date"].minute)

    # with open(pump_rates_file, 'r') as f:
    #    n_columns = read_data(f)
    #    fact_pumping = read_data(f)
    #    num_timesteps = read_data(f)
    #    repetition_frequency = read_data(f)
    #    dss_file = read_data(f)
    #    data = read_data(f)

    #    print(n_columns)
    #    print(fact_pumping)
    #    print(num_timesteps)
    #    print(repetition_frequency)
    #    print(dss_file)
    #    print(data[:72])
