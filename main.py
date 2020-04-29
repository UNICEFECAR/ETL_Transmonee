import os.path
import pandas as pd
import structure.structure
import tasks.transmonee_files.taskcfg
import tasks.transmonee_files.Transmonee_data

import tasks.unesco.Unesco_data
import tasks.eurostat.eurostat

import tasks.unesco.taskcfg as cfg_unesco

'''
The script downloads the data sources from APIs when available.
Transforms the data in a common format ready to be uplaoded in the SDMX data warehouse
'''

pd.set_option('display.min_rows', 50000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# TODO: as params
BASE_DIR = "Z:\\TransMonee\\01_getData"
DIR_dataDownload_UNESCO = BASE_DIR + "\\" + "dl\\unesco"
DIR_dataDownload_EUROSTAT = BASE_DIR + "\\" + "dl\\eurostat"
DIR_output = BASE_DIR + "\\" + "out"
OUT_FILE = "ETL_out.csv"

OUTPUT_DATAFLOW = "ECARO:TRANSMONEE(1.0)"

# the output format (similar to a SDMX Data Structure Definition)
dsd = [
    {"id": "Dataflow", "type": "string"},
    {"id": "REF_AREA", "type": "enum", "role": "dim",
     "codes": ["ALB", "ARM", "AZE", "BIH", "BLR", "GEO", "HRV", "KGZ", "KAZ", "MDA", "MNE", "MKD", "ROU", "SRB", "TJK",
               "TKM", "TUR", "UKR", "UZB"]},
    {"id": "UNICEF_INDICATOR", "type": "string", "role": "dim"},
    {"id": "SEX", "type": "string", "role": "dim"},
    {"id": "AGE", "type": "string", "role": "dim"},
    {"id": "RESIDENCE", "type": "string", "role": "dim", "codes": ["_T", "U", "R"]},
    # {"id": "WEALTH_QUINTILE", "type": "string", "role": "dim",
    #  "codes": ["_T", "Q1", "Q2", "Q3", "Q4", "Q5", "B20", "B40", "B60", "B80", "M40", "M60", "R20", "R40", "R60",
    #            "R80"]},
    {"id": "TIME_PERIOD", "type": "string", "role": "time"},
    {"id": "OBS_VALUE", "type": "string"},
    {"id": "UNIT_MEASURE", "type": "string"},
    {"id": "OBS_FOOTNOTE", "type": "string"},
    {"id": "FREQ", "type": "string"},
    {"id": "DATA_SOURCE", "type": "string"},
    {"id": "UNIT_MULTIPLIER", "type": "string"},
    {"id": "OBS_STATUS", "type": "string"},
]
# Create a structure object with the dsd.
struct = structure.structure.dsd(dsd)

# The destiantion dataframe
destination = pd.DataFrame(columns=struct.getCSVColumns(), dtype=str)


# Start processing UNESCO data: download and get destination-shaped data

# Filter functions, used to filter out some values form the datasets
def filterSDG4(df):
    # just keep the _T as socioeconomic background
    ret = df[(df["SE_BKGRD"] == "_T") | (df["SE_BKGRD"] == "_Z")]
    # just keep the _T as Immigration status
    ret = ret[(ret["IMM_STATUS"] == "_T") | (ret["IMM_STATUS"] == "_Z")]
    return ret


def filterEduNonFin(df):
    # just keep the _T as Education field
    ret = df[(df["EDU_FIELD"] == "_T") | (df["EDU_FIELD"] == "_Z")]
    # just keep the _T as Grade
    ret = ret[(ret["GRADE"] == "_T") | (ret["GRADE"] == "_Z")]
    # just keep the _T as EDU_TYPE
    ret = ret[(ret["EDU_TYPE"] == "_T") | (ret["EDU_TYPE"] == "_Z")]
    # just keep the _T as EDU_TYPE
    ret = ret[(ret["EDU_CAT"] == "_T") | (ret["EDU_CAT"] == "_Z")]
    return ret


def filterEduFin(df):
    # just keep the _T as Type of expenditure
    ret = df[df["EXPENDITURE_TYPE"] == "_T"]
    # just keep the _T as Funding Flow
    ret = ret[ret["FUND_FLOW"] == "_T"]

    return ret


# #Processing the SDG4 data
data = tasks.unesco.Unesco_data.getData(cfg_unesco.SOURCE_CONFIG_SDG4, DIR_dataDownload_UNESCO,
                                        {**cfg_unesco.country_map, **cfg_unesco.codemap_SDG4_EDUNONFIN},
                                        cfg_unesco.colmap_SDG4_EDUNONFIN,
                                        struct.getCSVColumns(), filterSDG4, skipIfExists=True, verb=3)
destination = destination.append(data)
# Processing the EDU NON FINANCE data
data = tasks.unesco.Unesco_data.getData(cfg_unesco.SOURCE_CONFIG_EDUNONFIN, DIR_dataDownload_UNESCO,
                                        {**cfg_unesco.country_map, **cfg_unesco.codemap_SDG4_EDUNONFIN},
                                        cfg_unesco.colmap_SDG4_EDUNONFIN,
                                        struct.getCSVColumns(), filterEduNonFin, skipIfExists=True, verb=3)
destination = destination.append(data)
# Processing the EDU FINANCE data
data = tasks.unesco.Unesco_data.getData(cfg_unesco.SOURCE_CONFIG_EDUFIN, DIR_dataDownload_UNESCO,
                                        {**cfg_unesco.country_map, **cfg_unesco.codemap_EDUFIN},
                                        cfg_unesco.colmap_EDUFIN,
                                        struct.getCSVColumns(), filterEduFin, skipIfExists=True, verb=3)
destination = destination.append(data)

# Adding the data contained in the TransMonEE excel files
for f in tasks.transmonee_files.taskcfg.files:
    path = os.path.join(BASE_DIR, f)
    data = tasks.transmonee_files.Transmonee_data.getData(path, tasks.transmonee_files.taskcfg.codemap,
                                                          tasks.transmonee_files.taskcfg.colmap,
                                                          tasks.transmonee_files.taskcfg.const)
    destination = destination.append(data)

# remove blanks, "-", ".."
destination.dropna(subset=["OBS_VALUE"], inplace=True)
destination = destination[destination["OBS_VALUE"] != "-"]
destination = destination[destination["OBS_VALUE"] != ".."]

# Add the constant dataflow id
destination['Dataflow'] = OUTPUT_DATAFLOW

# change the column order
destination.columns = struct.getCSVColumns()
# write the csv-sdmx
destination.to_csv(os.path.join(DIR_output, OUT_FILE), sep=",", header=True, encoding="utf-8", index=False)
