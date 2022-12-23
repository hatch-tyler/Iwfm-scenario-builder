import os
import numpy as np
import pandas as pd

def write_well_specifications(out_name, wellspec_df, wellchar_df, elem_groups, fact_xy=3.2808, fact_rw=1, fact_lt=1):
    """
    Write IWFM well specification data file

    Parameters
    ----------
    out_name : str
        out path and name of well specification data file

    wellspec_df : pd.DataFrame
        DataFrame object containing well specification information needed for IWFM

    wellchar_df : pd.DataFrame
        DataFrame object containing well characteristics needed for IWFM

    elem_groups : list
        list of element groups used to deliver water

    fact_xy : float default 3.2808
        conversion factor for well coordinates to model units

    fact_rw : float default 1.0
        conversion factor for well diameter

    fact_lt : float default 1.0
        conversion factor for perforation depths

    Returns
    -------
    None
        writes data to file
    """
    with open(out_name, "w") as f:
        f.write(
            "C*******************************************************************************\n"
        )
        f.write("C\n")
        f.write("C                  INTEGRATED WATER FLOW MODEL (IWFM)\n")
        f.write("C\n")
        f.write(
            "C*******************************************************************************\n"
        )
        f.write("C\n")
        f.write("C                        WELL SPECIFICATION FILE\n")
        f.write("C                           Pumping Component\n")
        f.write("C\n")
        f.write("C             Project:  C2VSim Fine Grid (C2VSimFG)\n")
        f.write(
            "C    California Central Valley Groundwater-Surface Water Simulation Model\n"
        )
        f.write("C             Filename: C2VSimFG_WellSpec.dat\n")
        f.write("C             Version:  C2VSimFG_v1.01     2021-03-04\n")
        f.write("C\n")
        f.write(
            "C*******************************************************************************\n"
        )
        f.write("C\n")
        f.write("C              ***** Version 1.0 Model Disclaimer *****\n")
        f.write("C\n")
        f.write(
            "C    This is Version 1.0 of C2VSimFG and is subject to change.  Users of\n"
        )
        f.write(
            "C    this version should be aware that this model is undergoing active\n"
        )
        f.write(
            "C    development and adjustment. Users of this model do so at their own\n"
        )
        f.write(
            "C    risk subject to the GNU General Public License below. The Department\n"
        )
        f.write(
            "C    does not guarantee the accuracy, completeness, or timeliness of the\n"
        )
        f.write(
            "C    information provided. Neither the Department of Water Resources nor\n"
        )
        f.write(
            "C    any of the sources of the information used by the Department in the\n"
        )
        f.write(
            "C    development of this model shall be responsible for any errors or\n"
        )
        f.write(
            "C    omissions, for the use, or results obtained from the use of this model.\n"
        )
        f.write("C\n")
        f.write(
            "C*******************************************************************************\n"
        )
        f.write("C\n")
        f.write(
            "C  California Central Valley Groundwater-Surface Water Flow Model - Fine Grid (C2VSimFG)\n"
        )
        f.write("C  Copyright (C) 2012-2021\n")
        f.write("C  State of California, Department of Water Resources\n")
        f.write("C\n")
        f.write("C  This model is free. You can redistribute it and/or modify it\n")
        f.write("C  under the terms of the GNU General Public License as published\n")
        f.write(
            "C  by the Free Software Foundation; either version 2 of the License,\n"
        )
        f.write("C  or (at your option) any later version.\n")
        f.write("C\n")
        f.write("C  This model is distributed WITHOUT ANY WARRANTY; without even\n")
        f.write("C  the implied warranty of MERCHANTABILITY or FITNESS FOR A\n")
        f.write("C  PARTICULAR PURPOSE.  See the GNU General Public License for\n")
        f.write("C  more details. (http://www.gnu.org/licenses/gpl.html)\n")
        f.write("C\n")
        f.write(
            "C  The GNU General Public License is available from the Free Software\n"
        )
        f.write(
            "C  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.\n"
        )
        f.write("C\n")
        f.write("C  For technical support, e-mail: C2VSimFGtechsupport@water.ca.gov\n")
        f.write("C\n")
        f.write("C    C2VSimFG/SGMA Contact:\n")
        f.write("C          Tyler Hatch, PhD, PE, Supervising Engineer, DWR\n")
        f.write("C          (916) 651-7014, tyler.hatch@water.ca.gov\n")
        f.write("C\n")
        f.write("C    IWFM Contact:\n")
        f.write("C          Emin Can Dogrul PhD, PE, Senior Engineer, DWR\n")
        f.write("C          (916) 654-7018, can.dogrul@water.ca.gov\n")
        f.write("C\n")
        f.write(
            "C*******************************************************************************\n"
        )
        f.write("C                             File Description:\n")
        f.write("C\n")
        f.write(
            "C   This data file includes the relevant data for the wells that are simulated\n"
        )
        f.write("C   in the model.\n")
        f.write("C\n")
        f.write(
            "C*******************************************************************************\n"
        )
        f.write(
            "C              List of modeled wells and their corresponding parameters\n"
        )
        f.write("C\n")
        f.write("C   NWELL ; Number of wells modeled\n")
        f.write("C   FACTXY; Conversion factor for well coordinates\n")
        f.write("C   FACTRW; Conversion factor for well diameter\n")
        f.write("C   FACTLT; Conversion factor for perforation depths\n")
        f.write("C\n")
        f.write(
            "C-------------------------------------------------------------------------------\n"
        )
        f.write("C     VALUE                       DESCRIPTION\n")
        f.write(
            "C-------------------------------------------------------------------------------\n"
        )
        f.write(" " * 6 + "{:<28d}".format(len(wellspec_df)) + "/ NWELL\n")
        f.write(" " * 6 + "{:<28.4f}".format(fact_xy) + "/ FACTXY\n")
        f.write(" " * 6 + "{:<28d}".format(fact_rw) + "/ FACTRW\n")
        f.write(" " * 6 + "{:<28d}".format(fact_lt) + "/ FACTLT\n")
        f.write(
            "C-------------------------------------------------------------------------------\n"
        )
        f.write(
            "C*******************************************************************************\n"
        )
        f.write("C                    Well Location and Structure Characteristics\n")
        f.write("C\n")
        f.write("C   ID      ;  Well identification number\n")
        f.write("C   XWELL   ;  X coordinate of well location, [L]\n")
        f.write("C   YWELL   ;  Y-coordinate of well location, [L]\n")
        f.write("C   RWELL   ;  Well diameter, [L]\n")
        f.write("C   PERFT   ;  Elevation of or depth to the top of well screen, [L]\n")
        f.write(
            "C   PERFB   ;  Elevation of or depth to the bottom of well screen, [L]\n"
        )
        f.write(
            "C                 *** Note: If PERFT > PERFB screening interval is given as elevations\n"
        )
        f.write(
            "C                           IF PERFT < PERFB screening interval is given as depth-to top/bottom of screening\n"
        )
        f.write("C\n")
        f.write(
            "C-------------------------------------------------------------------------------\n"
        )
        f.write("C    ID       XWELL     YWELL       RWELL      PERFT      PERFB\n")
        f.write(
            "C-------------------------------------------------------------------------------\n"
        )
        wellspec_df.to_string(
            f,
            header=False,
            index=False,
            formatters={
                "ID": "{:>7d}".format,
                "XWELL": "{:>12.3f}".format,
                "YWELL": "{:>13.3f}".format,
                "RWELL": "{:>3.1f}".format,
                "PERFT": "{:>12.7f}".format,
                "PERFB": "{:>12.7f}".format
            }
        )
        f.write("\n")
        f.write(
            "C*******************************************************************************\n"
        )
        f.write("C                        Well Pumping Characteristics\n")
        f.write("C\n")
        f.write("C   ID       ; Well identification number\n")
        f.write(
            "C   ICOLWL   ; Well pumping - this number corresponds to the appropriate data column\n"
        )
        f.write("C               in the Time Series Pumping File\n")
        f.write(
            "C               * Enter 0 if pumping will not be read from the Time Series\n"
        )
        f.write(
            "C                  Pumping File (for instance, in a situation where pumping is \n"
        )
        f.write("C                  calculated by an external program)\n")
        f.write(
            "C   FRACWL   ; Relative proportion of the pumping in column ICOLWL to be applied \n"
        )
        f.write("C               to well ID\n")
        f.write(
            "C   IOPTWL   ; Option for distribution of pumping at the delivery destination\n"
        )
        f.write(
            "C               0 = to distribute the pumping according to the given relative\n"
        )
        f.write("C                     fraction, FRACWL\n")
        f.write(
            "C               1 = to distribute the pumping in proportion to FRACWL\n"
        )
        f.write(
            "C                     times the total area of the destination for pumping\n"
        )
        f.write(
            "C               2 = to distribute the pumping in proportion to FRACWL\n"
        )
        f.write(
            "C                     times the developed area (ag. and urban) at the destination\n"
        )
        f.write("C                     for pumping\n")
        f.write(
            "C               3 = to distribute the pumping in proportion to FRACWL\n"
        )
        f.write(
            "C                     times the ag. area at the destination for pumping\n"
        )
        f.write(
            "C               4 = to distribute the pumping in proportion to FRACWL\n"
        )
        f.write(
            "C                     times the urban area at the destination for pumping\n"
        )
        f.write("C   TYPDSTWL ; Destination where the pumping is delivered to \n")
        f.write(
            "C              -1 = pumping is used in the same element that pumping occurs\n"
        )
        f.write("C               0 = Pumping goes outside the model domain\n")
        f.write("C               2 = Pumping goes to element DSTWL (see below)\n")
        f.write("C               4 = Pumping goes to subregion DSTWL (see below)\n")
        f.write(
            "C               6 = Pumping goes to a group of elements with ID DSTDL\n"
        )
        f.write(
            "C                     (element groups are listed after this section)\n"
        )
        f.write("C   DSTWL    ; Destination number for well pumping delivery\n")
        f.write("C               * Enter any number if TYPDSTWL is set to -1 or 0\n")
        f.write(
            "C   ICFIRIGWL; Fraction of the pumping that is used for irrigation purposes -\n"
        )
        f.write(
            "C               this number corresponds to the appropriate data column in the\n"
        )
        f.write("C               Irrigation Fractions Data File\n")
        f.write(
            "C               * Enter 0 if pumping is delivered outside the model domain\n"
        )
        f.write(
            "C   ICADJWL  ; Supply adjustment specification - this number corresponds to\n"
        )
        f.write(
            "C               the data column in the Supply Adjustment Specifications\n"
        )
        f.write("C               Data File\n")
        f.write("C               * Enter 0 if well pumping will not be adjusted\n")
        f.write(
            "C   ICWLMAX  ; Maximum pumping amount - this number corresponds to the\n"
        )
        f.write(
            "C               appropriate data column in the Time Series Pumping File\n"
        )
        f.write(
            "C               * Enter 0 if a maximum pumping amount does not apply\n"
        )
        f.write(
            "C   FWLMAX   ; Fraction of data value specified in column ICWLMAX to be used as\n"
        )
        f.write("C               maximum pumping amount\n")
        f.write("C\n")
        f.write(
            "C--------------------------------------------------------------------------------------------------\n"
        )
        f.write(
            "C    ID      ICOLWL   FRACWL    IOPTWL   TYPDSTWL    DSTWL   ICFIRIGWL   ICADJWL  ICWLMAX   FWLMAX\n"
        )
        f.write(
            "C--------------------------------------------------------------------------------------------------\n"
        )
        wellchar_df.to_string(
            f,
            header=False,
            index=False,
            formatters={
                "ID": "{:>7d}".format,
                "ICOLWL": "{:>11d}".format,
                "FRACWL": "{:>8.1f}".format,
                "IOPTWL": "{:>9d}".format,
                "TYPDSTWL": "{:>10d}".format,
                "DSTWL": "{:>8d}".format,
                "ICFIRIGWL": "{:>11d}".format,
                "ICADJWL": "{:>9d}".format,
                "ICWLMAX": "{:>8d}".format,
                "FWLMAX": "{:>8.1f}".format,
            }
        )
        f.write("\n")
        f.write(
            "C--------------------------------------------------------------------------------------------------\n"
        )
        f.write("C\n")
        f.write("C                Element Groups for Well Pumping Deliveries\n")
        f.write("C\n")
        f.write(
            "C   List the elements in each group where selected well pumping above is delivered to. All \n"
        )
        f.write("C   elements in each group must belong to the same subregion.\n")
        f.write("C\n")
        f.write("C   NGRP  ; Number of element groups \n")
        f.write(
            "C            * Enter 0 if there are no element groups where well pumping is delivered\n"
        )
        f.write("C   ID    ; Element group ID entered sequentially\n")
        f.write("C   NELEM ; Number of elements in element group ID\n")
        f.write("C   IELEM ; Element numbers that are in group ID\n")
        f.write("C\n")
        f.write(
            "C-------------------------------------------------------------------------------\n"
        )
        f.write("      33                 / NGRP\n")
        f.write(
            "C-------------------------------------------------------------------------------\n"
        )
        f.write("C    ID         NELEM      IELEM\n")
        f.write(
            "C-------------------------------------------------------------------------------\n"
        )
        for line in elem_groups:
            f.write(line)