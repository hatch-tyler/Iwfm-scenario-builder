import re
import numpy as np
import pandas as pd

from typing import List
from utilities import read_data


class WellProperties:
    def __init__(
        self,
        well_id: int,
        x: float,
        y: float,
        diameter: float,
        top_perf: float,
        bottom_perf: float,
    ):
        self.well_id = well_id
        self.x = x
        self.y = y
        self.diameter = diameter
        self.top_perf = top_perf
        self.bottom_perf = bottom_perf

    def __repr__(self):
        return f"WellProperties(well_id={self.well_id}, x={self.x}, y={self.y}, diameter={self.diameter}, top_perf={self.top_perf}, bottom_perf={self.bottom_perf})"

    def __str__(self):
        return f"WellProperties(well_id={self.well_id}, x={self.x}, y={self.y}, diameter={self.diameter}, top_perf={self.top_perf}, bottom_perf={self.bottom_perf})"

    def to_dict(self):
        return dict(
            ID=self.well_id,
            XWELL=self.x,
            YWELL=self.y,
            RWELL=self.diameter,
            PERFT=self.top_perf,
            PERFB=self.bottom_perf,
        )


class WellPumpingConfig:
    def __init__(
        self,
        well_id: int,
        col_pumping: int,
        pumping_fraction: float,
        well_option: int,
        well_destination_type: int,
        well_destination: int,
        col_irrig_fraction: int,
        col_supply_adjust: int,
        col_max_pumping: int,
        max_pumping_fraction: float,
    ):
        self.well_id = well_id
        self.col_pumping = col_pumping
        self.pumping_fraction = pumping_fraction
        self.well_option = well_option
        self.well_destination_type = well_destination_type
        self.well_destination = well_destination
        self.col_irrig_fraction = col_irrig_fraction
        self.col_supply_adjust = col_supply_adjust
        self.col_max_pumping = col_max_pumping
        self.max_pumping_fraction = max_pumping_fraction

    def __repr__(self):
        return f"WellPumpingConfig(well_id={self.well_id}, col_pumping={self.col_pumping}, pumping_fraction={self.pumping_fraction}, well_option={self.well_option}, well_destination_type={self.well_destination_type}, well_destination={self.well_destination}, col_irrig_fraction={self.col_irrig_fraction}, col_supply_adjust={self.col_supply_adjust}, col_max_pumping={self.col_max_pumping}, max_pumping_fraction={self.max_pumping_fraction}"

    def to_dict(self):
        return dict(
            ID=self.well_id,
            ICOLWL=self.col_pumping,
            FRACWL=self.pumping_fraction,
            IOPTWL=self.well_option,
            TYPDSTWL=self.well_destination_type,
            DSTWL=self.well_destination,
            ICFIRIGWL=self.col_irrig_fraction,
            ICADJWL=self.col_supply_adjust,
            ICWLMAX=self.col_max_pumping,
            FWLMAX=self.max_pumping_fraction,
        )


class ElementGroup:
    def __init__(self, elemgrp_id: int, n_elements: int, elements: List[int]):
        self.elemgrp_id = elemgrp_id
        self.n_elements = n_elements
        self.elements = elements

    def __repr__(self):
        return (
            f"ElementGroup(elemgrp_id={self.elemgrp_id}, n_elements={self.n_elements})"
        )

    def to_string_list(self, end="\n") -> list:
        """
        Convert object to list of strings

        Parameters
        ----------
        end : str default '\n'
            end character for string

        Returns
        -------
        list
            list of strings representing the element group
        """
        elem_grp_list = []
        for i, e in enumerate(self.elements):
            if i == 0:
                elem_grp_list.append(
                    f"{self.elemgrp_id:>7d}{self.n_elements:>14d}{e:>11d}{end}"
                )
            else:
                elem_grp_list.append(f"{e:>32d}{end}")

        return elem_grp_list


class IWFMWells:
    def __init__(
        self,
        n_wells: float,
        fact_xy: float,
        fact_rw: float,
        fact_lt: float,
        properties: List[WellProperties],
        pump_config: List[WellPumpingConfig],
        n_groups: int,
        element_groups: List[ElementGroup],
    ):
        self.n_wells = n_wells
        self.fact_xy = fact_xy
        self.fact_rw = fact_rw
        self.fact_lt = fact_lt
        self.properties = properties
        self.pump_config = pump_config
        self.n_groups = n_groups
        self.element_groups = element_groups

    def get_properties(self) -> list:
        return self.properties

    def get_pump_config(self) -> list:
        return self.pump_config

    def get_properties_as_dict(self) -> dict:
        wi = self.get_properties()
        ws = [w.to_dict() for w in wi]

        return {k: [ws_dict[k] for ws_dict in ws] for k in ws[0]}

    def get_property_names(self) -> list:
        names = list(self.get_properties_as_dict().keys())
        return names

    def get_pump_config_as_dict(self) -> dict:
        wc = self.get_pump_config()
        wp = [w.to_dict() for w in wc]

        return {k: [wc_dict[k] for wc_dict in wp] for k in wp[0]}

    def get_pump_config_names(self) -> list:
        names = list(self.get_pump_config_as_dict().keys())
        return names

    def to_dataframe(self, which="all") -> pd.DataFrame:
        if not isinstance(which, str):
            raise TypeError("variable 'which' must be a string")

        if which.lower() not in ["all", "properties", "pump_config"]:
            raise ValueError("Value entered is not a valid option.")

        wp_df = pd.DataFrame(self.get_properties_as_dict())
        wc_df = pd.DataFrame(self.get_pump_config_as_dict())

        if which == "all":
            return pd.merge(wp_df, wc_df, on="ID")

        if which == "properties":
            return wp_df

        if which == "pump_config":
            return wc_df

    def get_element_groups_as_list(self) -> list:
        grp_list = []
        for grp in self.element_groups:
            grp_list += grp.to_string_list()

        return grp_list

    @classmethod
    def from_file(cls, file_name):
        with open(file_name, "r") as f:
            n_wells = int(read_data(f))
            fact_xy = float(read_data(f))
            fact_rw = float(read_data(f))
            fact_lt = float(read_data(f))

            all_well_props = []
            for i in range(n_wells):
                well_props = read_data(f)

                well_props = well_props.split()

                if len(well_props) != 6:
                    raise ValueError("Values do not correctly specify well information")

                well_id = int(well_props[0])
                x = float(well_props[1])
                y = float(well_props[2])
                diameter = float(well_props[3])
                top_perf = float(well_props[4])
                bottom_perf = float(well_props[5])

                well = WellProperties(well_id, x, y, diameter, top_perf, bottom_perf)
                all_well_props.append(well)

            all_pump_config = []
            for i in range(n_wells):
                well_pump_config = read_data(f)

                well_pump_config = well_pump_config.split()

                if len(well_pump_config) != 10:
                    raise ValueError(
                        "Values do not correctly specify well characteristics"
                    )

                well_id = int(well_pump_config[0])
                col_pumping = int(well_pump_config[1])
                pumping_fraction = float(well_pump_config[2])
                well_option = int(well_pump_config[3])
                well_destination_type = int(well_pump_config[4])
                well_destination = int(well_pump_config[5])
                col_irrig_fraction = int(well_pump_config[6])
                col_supply_adjust = int(well_pump_config[7])
                col_max_pumping = int(well_pump_config[8])
                max_pumping_fraction = float(well_pump_config[9])

                well_pumping_config = WellPumpingConfig(
                    well_id,
                    col_pumping,
                    pumping_fraction,
                    well_option,
                    well_destination_type,
                    well_destination,
                    col_irrig_fraction,
                    col_supply_adjust,
                    col_max_pumping,
                    max_pumping_fraction,
                )
                all_pump_config.append(well_pumping_config)

            n_grp = int(read_data(f))

            element_groups = []
            for grp in range(n_grp):
                group_spec = read_data(f)
                group_spec = group_spec.split()

                # check first line of element group has 3 values
                if len(group_spec) != 3:
                    raise ValueError("Values do not correctly specify an Element Group")

                grp_id = int(group_spec[0])
                n_elements = int(group_spec[1])
                elements = []
                elements.append(int(group_spec[2]))
                for i in range(n_elements - 1):
                    elements.append(int(read_data(f)))

                element_group = ElementGroup(grp_id, n_elements, elements)
                element_groups.append(element_group)

        return cls(
            n_wells,
            fact_xy,
            fact_rw,
            fact_lt,
            all_well_props,
            all_pump_config,
            n_grp,
            element_groups,
        )


if __name__ == "__main__":
    wellspec_file = "Simulation/Groundwater/C2VSimFG_WellSpec.dat"
    c2vsimfg_wells = IWFMWells.from_file(wellspec_file)

    wells = c2vsimfg_wells.to_dataframe()

    print(wells.head())

    element_groups = c2vsimfg_wells.get_element_groups_as_list()

    for grp in element_groups:
        print(grp, end="")

    print(c2vsimfg_wells.get_property_names())
    print(c2vsimfg_wells.get_pump_config_names())
