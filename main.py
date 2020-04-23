import os.path
import pandas as pd
import structure.structure

import tasks.unesco.unesco
import tasks.eurostat.eurostat

'''
The script downloads the data sources from APIs when available.
Transforms the data in a common format ready to be uplaoded in the SDMX data warehouse
'''

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# TODO: as params
BASE_DIR = "Z:\\TransMonee\\01_getData"
DIR_dataDownload_UNESCO = BASE_DIR + "\\" + "dl\\unesco"
DIR_dataDownload_EUROSTAT = BASE_DIR + "\\" + "dl\\eurostat"
DIR_output = BASE_DIR + "\\" + "out"
OUT_FILE = "ETL_out.csv"

UNESCO_SOURCE_CONFIG_SDG4 = "source_configs\\unesco\\EDU_UIS_SDG4_toDown.csv"
UNESCO_SOURCE_CONFIG_EDUNONFIN = "source_configs\\unesco\\EDU_UIS_EDUNONFIN_toDown.csv"

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
# UNESCO SDG4 dataflow
def filterSDG4(df):
    # just keep the _T as socioeconomic background
    ret = df[df["SE_BKGRD"].str.contains("_T") | df["SE_BKGRD"].str.contains("_Z")]
    # just keep the _T as Immigration status
    ret = ret[ret["IMM_STATUS"].str.contains("_T") | ret["IMM_STATUS"].str.contains("_Z")]
    return ret


task = tasks.unesco.unesco.UNESCO(UNESCO_SOURCE_CONFIG_SDG4)
task.download_data(DIR_dataDownload_UNESCO, True, verb=3)
srcData = task.getdata(DIR_dataDownload_UNESCO, struct.getCSVColumns(), filterFunction=filterSDG4)
destination = destination.append(srcData)
# UNESCO EDUNonFinance dataflow dataflow
task = tasks.unesco.unesco.UNESCO(UNESCO_SOURCE_CONFIG_EDUNONFIN)
task.download_data(DIR_dataDownload_UNESCO, True, verb=3)
srcData = task.getdata(DIR_dataDownload_UNESCO, struct.getCSVColumns(), filterFunction=None)
destination = destination.append(srcData)

# tasks.unesco.unesco.download_data(DIR_dataDownload_UNESCO, True, verb=3)
# srcData = tasks.unesco.unesco.getdata(DIR_dataDownload_UNESCO, struct.getCSVColumns())
# destination = destination.append(srcData)

# Start processing EUROSTAT data: download and get destination-shaped data
# tasks.eurostat.eurostat.download_data(DIR_dataDownload_EUROSTAT, skipIfExists=True, verb=3)
# srcData = tasks.eurostat.eurostat.getdata(DIR_dataDownload_EUROSTAT, struct.getCSVColumns())
# destination = destination.append(srcData)

duplicates = destination[destination.duplicated(subset=struct.get_dims(), keep=False)]
print(duplicates)

destination.to_csv(os.path.join(DIR_output, OUT_FILE), sep=",", header=True, encoding="utf-8", index=False)
