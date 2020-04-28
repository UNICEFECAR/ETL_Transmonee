import os.path
import pandas as pd
import structure.structure
import tasks.transmonee_files.taskcfg
import tasks.transmonee_files.Transmonee_data

import tasks.unesco.unesco
import tasks.eurostat.eurostat

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

UNESCO_SOURCE_CONFIG_SDG4 = "source_configs\\unesco\\EDU_UIS_SDG4_toDown.csv"
UNESCO_SOURCE_CONFIG_EDUNONFIN = "source_configs\\unesco\\EDU_UIS_EDUNONFIN_toDown.csv"
UNESCO_SOURCE_CONFIG_EDUFIN = "source_configs\\unesco\\EDU_UIS_EDUFIN_toDown.csv"

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
    {"id": "WEALTH_QUINTILE", "type": "string", "role": "dim",
     "codes": ["_T", "Q1", "Q2", "Q3", "Q4", "Q5", "B20", "B40", "B60", "B80", "M40", "M60", "R20", "R40", "R60",
               "R80"]},
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
# The mapping between the data and the DSD columns
colMap_UNESCO_SDG4_EDUNONFIN = {

    "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
    "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
    "SEX": {"type": "col", "role": "dim", "value": "SEX"},
    "AGE": {"type": "col", "role": "dim", "value": "AGE"},
    "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
    "RESIDENCE": {"type": "col", "role": "dim", "value": "LOCATION"},
    "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
    "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
    "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
    "OBS_FOOTNOTE": {"type": "const", "role": "attrib", },
    "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
    "DATA_SOURCE": {"type": "const", "role": "attrib", "value": "UIS"},
    "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
    "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
}
# the country mappinh gor unesco
UNESCO_COUNTRY_MAP = {"REF_AREA": {
    "AL": "ALB",
    "AM": "ARM",
    "AZ": "AZE",
    "BA": "BIH",
    "BG": "BGR",
    "BY": "BLR",
    "CZ": "CZE",
    "EE": "EST",
    "GE": "GEO",
    "HR": "HRV",
    "HU": "HUN",
    "KG": "KGZ",
    "KZ": "KAZ",
    "LT": "LTU",
    "LV": "LVA",
    "MD": "MDA",
    "ME": "MNE",
    "MK": "MKD",
    "PL": "POL",
    "RO": "ROU",
    "RS": "SRB",
    "RU": "RUS",
    "SI": "SVN",
    "SK": "SVK",
    "TJ": "TJK",
    "TM": "TKM",
    "TR": "TUR",
    "UA": "UKR",
    "UZ": "UZB",
}}
# Additional mappings for SDG4 and EDUNONFIN
UNESCO_CODEMAP_SDG4_EDUNONFIN = {
    "UNIT_MEASURE": {
        "PER": "PS",
        "PT": "PCNT",
    },
    "AGE": {
        "UNDER1_AGE": "M023",
    },
    "LOCATION": {
        "RUR": "R",
        "URB": "U",
        "_Z": "_T",
        "_T": "_T"
    },
    "WEALTH_QUINTILE": {
        "_Z": "_T"
    }
}


# UNESCO SDG4 dataflow
# the data filter function
def filterSDG4(df):
    # just keep the _T as socioeconomic background
    ret = df[(df["SE_BKGRD"] == "_T") | (df["SE_BKGRD"] == "_Z")]
    # just keep the _T as Immigration status
    ret = ret[(ret["IMM_STATUS"] == "_T") | (ret["IMM_STATUS"] == "_Z")]
    return ret


task = tasks.unesco.unesco.UNESCO(UNESCO_SOURCE_CONFIG_SDG4)
task.download_data(DIR_dataDownload_UNESCO, True, verb=3)

srcData = task.getdata(DIR_dataDownload_UNESCO, struct.getCSVColumns(), colMap_UNESCO_SDG4_EDUNONFIN,
                       codeMap={**UNESCO_COUNTRY_MAP, **UNESCO_CODEMAP_SDG4_EDUNONFIN}, filterFunction=filterSDG4)
destination = destination.append(srcData)


# UNESCO EDUNonFinance dataflow
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


task = tasks.unesco.unesco.UNESCO(UNESCO_SOURCE_CONFIG_EDUNONFIN)

task.download_data(DIR_dataDownload_UNESCO, True, verb=3)
srcData = task.getdata(DIR_dataDownload_UNESCO, struct.getCSVColumns(), colMap_UNESCO_SDG4_EDUNONFIN,
                       codeMap={**UNESCO_COUNTRY_MAP, **UNESCO_CODEMAP_SDG4_EDUNONFIN}, filterFunction=filterEduNonFin)
destination = destination.append(srcData)

# UNESCO EDUFinance dataflow
UNESCO_CODEMAP_EDUFIN = {
    "UNIT_MEASURE": {"GDP": "GDP_PERC"}

}
colMap_UNESCO_EDUFIN = {
    "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
    "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
    "SEX": {"type": "const", "role": "dim"},
    "AGE": {"type": "const", "role": "dim"},
    "WEALTH_QUINTILE": {"type": "const", "role": "dim"},
    "RESIDENCE": {"type": "const", "role": "dim"},
    "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
    "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
    "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
    "OBS_FOOTNOTE": {"type": "const", "role": "attrib", },
    "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
    "DATA_SOURCE": {"type": "const", "role": "attrib", "value": "UIS"},
    "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
    "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
}


def filterEduFin(df):
    # just keep the _T as Type of expenditure

    ret = df[df["EXPENDITURE_TYPE"] == "_T"]
    # just keep the _T as Funding Flow
    ret = ret[ret["FUND_FLOW"] == "_T"]

    return ret


task = tasks.unesco.unesco.UNESCO(UNESCO_SOURCE_CONFIG_EDUFIN)
task.download_data(DIR_dataDownload_UNESCO, True, verb=3)
srcData = task.getdata(DIR_dataDownload_UNESCO, struct.getCSVColumns(), colMap_UNESCO_EDUFIN,
                       codeMap={**UNESCO_COUNTRY_MAP, **UNESCO_CODEMAP_EDUFIN}, filterFunction=filterEduFin)
destination = destination.append(srcData)

duplicates = destination[destination.duplicated(subset=struct.get_dims(), keep=False)]
if duplicates.empty:
    print("No duplicates found")
else:
    duplicates = duplicates[struct.get_dims()]
    # print(duplicates)

# remove blanks
destination.dropna(subset=["OBS_VALUE"], inplace=True)

destination['Dataflow'] = "ECARO:TRANSMONEE(1.0)"
# change the column order
destination.columns = struct.getCSVColumns()

destination.to_csv(os.path.join(DIR_output, OUT_FILE), sep=",", header=True, encoding="utf-8", index=False)

destination2 = pd.DataFrame(columns=struct.getCSVColumns(), dtype=str)

for f in tasks.transmonee_files.taskcfg.files:
    path = os.path.join(BASE_DIR, f)
    # task = tasks.transmonee_files.Transmonee_data.TransmoneeData()
    # data = task.readSource(path)
    # data = task.map_codes(data, tasks.transmonee_files.taskcfg.codemap)
    # data = task.mapcols(data, tasks.transmonee_files.taskcfg.colmap, tasks.transmonee_files.taskcfg.const)
    data = tasks.transmonee_files.Transmonee_data.getData(path, tasks.transmonee_files.taskcfg.codemap,
                                                          tasks.transmonee_files.taskcfg.colmap,
                                                          tasks.transmonee_files.taskcfg.const)
    destination2 = destination2.append(data)

destination2.to_csv(os.path.join(DIR_output, "tm.csv"), sep=",", header=True, encoding="utf-8", index=False)
