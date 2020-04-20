import os.path
import pandas as pd
import structure.structure

import tasks.unesco.unesco
import tasks.eurostat.eurostat

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

#TODO: as params
BASE_DIR = "Z:\\TransMonee\\01_getData"
DIR_dataDownload_UNESCO = BASE_DIR + "\\" + "dl\\unesco"
DIR_dataDownload_EUROSTAT = BASE_DIR + "\\" + "dl\\eurostat"
DIR_output = BASE_DIR + "\\" + "out"
OUT_FILE = "ETL_out.csv"

#the output format
dsd = [
    {"id": "Dataflow", "type": "string"},
    {"id": "REF_AREA", "type": "enum",
     "codes": ["ALB", "ARM", "AZE", "BIH", "BLR", "GEO", "HRV", "KGZ", "KAZ", "MDA", "MNE", "MKD", "ROU", "SRB", "TJK",
               "TKM", "TUR", "UKR", "UZB"]},
    {"id": "UNICEF_INDICATOR", "type": "string"},
    {"id": "SEX", "type": "string"},
    {"id": "AGE", "type": "string"},
    {"id": "TIME_PERIOD", "type": "string"},
    {"id": "OBS_VALUE", "type": "string"},
    {"id": "UNIT_MEASURE", "type": "string"},
    {"id": "OBS_FOOTNOTE", "type": "string"},
    {"id": "FREQ", "type": "string"},
    {"id": "DATA_SOURCE", "type": "string"},
    {"id": "UNIT_MULTIPLIER", "type": "string"},
    {"id": "OBS_STATUS", "type": "string"},
]
#Create a structure object with the dsd.
struct = structure.structure.dsd(dsd)

destination = pd.DataFrame(columns=struct.getCSVColumns(), dtype=str)
# UNESCO
tasks.unesco.unesco.download_data(DIR_dataDownload_UNESCO, True, verb=3)
srcData = tasks.unesco.unesco.getdata(DIR_dataDownload_UNESCO, struct.getCSVColumns())
destination = destination.append(srcData)

# EUROSTAT
tasks.eurostat.eurostat.download_data(DIR_dataDownload_EUROSTAT, skipIfExists=True, verb=3)
srcData = tasks.eurostat.eurostat.getdata(DIR_dataDownload_EUROSTAT, struct.getCSVColumns())
destination = destination.append(srcData)

# print(destination)
destination.to_csv(os.path.join(DIR_output, "ETL_out_2.csv"), sep=",", header=True, encoding="utf-8", index=False)
