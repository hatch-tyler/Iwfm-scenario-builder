import os
import pandas as pd
from scenario_builder.scenario_builder import scenario_builder

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

    scenario_ids = projects["Scenario"].tolist()

    # loop through each project to generate the pumping timeseries file
    for scenario_id in scenario_ids:

        scenario_builder(
            scenario_id,
            projects,
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
