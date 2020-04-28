import os.path
import pandas as pd
import structure.structure
import numpy as np

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

import tasks.transmonee_files.Transmonee_data

colMap_tm = {
    "REF_AREA": {"type": "col", "role": "dim", "value": "country"},
    "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "indicator"},
    "SEX": {"type": "const", "role": "dim"},
    "AGE": {"type": "const", "role": "dim"},
    "WEALTH_QUINTILE": {"type": "const", "role": "dim"},
    "RESIDENCE": {"type": "const", "role": "dim"},
    "TIME_PERIOD": {"type": "col", "role": "time", "value": "year"},
    "OBS_VALUE": {"type": "col", "role": "obs", "value": "value"},
    "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "unit"},
    "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "note"},
    "FREQ": {"type": "const", "role": "attrib"},
    "DATA_SOURCE": {"type": "const", "role": "attrib"},
    "UNIT_MULTIPLIER": {"type": "const", "role": "attrib"},
    "OBS_STATUS": {"type": "const", "role": "attrib"},
}

const = {
    "SEX": "_T",
    "AGE": "_T",
    "WEALTH_QUINTILE": "_T",
    "RESIDENCE": "_T",
    "FREQ": "",
    "DATA_SOURCE": "",
    "UNIT_MULTIPLIER": "",
    "OBS_STATUS": ""
}

transmoneeCodeMap = {
    "unit":
        {"depends": "indicator",
         "map": {
             "6.1.1 Total number of children left without parental care (during the year)": "NUMBER",
             "6.1.2 Rate of children left without parental care (during the year, per 100,000 average population aged 0-17)": "PCNT",
             "6.1.3 Children left without parental care by sex: boys (during the year)": "NUMBER",
             "6.1.4 Children left without parental care by sex: girls (during the year)": "NUMBER",
             "6.1.5 Children left without parental care by age: 0-2 year olds (during the year)": "NUMBER",
             "6.1.6 Children left without parental care by age: 3-6 year olds (during the year)": "NUMBER",
             "6.1.7 Children left without parental care by age: 7-17 year olds (during the year)": "NUMBER",
             "6.1.8 Children with disabilities left without parental care (during the year)": "NUMBER",
             "6.1.9 Children with disabilities left without parental care by sex: boys (during the year)": "NUMBER",
             "6.1.10 Children with disabilities left without parental care by sex: girls (during the year)": "NUMBER",
             "6.1.11 Children left without parental care by cause: orphan children (during the year)": "NUMBER",
             "6.1.12 Children left without parental care by cause: deprivation of parental rights (during the year)": "NUMBER",
             "6.1.13 Children left without parental care by cause: abandonment or relinquishment by the parents (during the year)": "NUMBER",
             "6.1.14 Children left without parental care by cause: parents temporary unable/ not in a position to care for the child (during the year)": "NUMBER",
             "6.1.15 Children left without parental care by cause: other (during the year)": "NUMBER",
             "6.1.16 Children left without parental care placed into care (during the year)": "NUMBER",
             "6.1.17 Children left without parental care by type of placement: in residential care (during the year)": "NUMBER",
             "6.1.18 Children left without parental care by type of placement: entered educational institution (during the year)": "NUMBER",
             "6.1.19 Children left without parental care by type of placement: entered foster care (during the year)": "NUMBER",
             "6.1.20 Children left without parental care by type of placement: entered guardian care (during the year)": "NUMBER",
             "6.1.21 Children left without parental care by type of placement: were adopted (during the year)": "NUMBER",
             "6.1.22 Total number of children in formal care (at the end of the year)": "NUMBER",
             "6.1.23 Rate of children in formal care (at the end of the year, per 100,000 population aged 0-17)": "PCNT",
             "6.2.1 Total number of children in residential care (at the end of the year)": "NUMBER",
             "6.2.2 Rate of children in residential care (at the end of the year, per 100,000 population aged  0-17)": "PCNT",
             "6.2.3 Children in residential care by sex: boys (at the end of the year)": "NUMBER",
             "6.2.4 Children in residential care by sex: girls (at the end of the year)": "NUMBER",
             "6.2.5 Children in residential care by age: 0-2 years olds (at the end of the year)": "NUMBER",
             "6.2.6 Children in residential care by age: 3-6 years olds (at the end of the year)": "NUMBER",
             "6.2.7 Children in residential care by age: 7-17 years olds (at the end of the year)": "NUMBER",
             "6.2.8 Children in residential care by age: 18 years and older (at the end of the year)": "NUMBER",
             "6.2.9 Number of children with disabilities in public residential care - all types of institutions (at the end of the year)": "NUMBER",
             "6.2.10 Children with disabilities in public residential care by sex: boys (at the end of the year)": "NUMBER",
             "6.2.11 Children with disabilities in public residential care by sex: girls (at the end of the year)": "NUMBER",
             "6.2.12 Children with disability in public residential care by age: 0-2 years olds (at the end of the year)": "NUMBER",
             "6.2.13 Children with disability in public residential care by age: 3-6 years olds (at the end of the year)": "NUMBER",
             "6.2.14 Children with disability in public residential care by age: 7-17 years olds (at the end of the year)": "NUMBER",
             "6.2.15 Children with disability in public residential care by age: 18 years old and elder (at the end of the year)": "NUMBER",
             "6.2.16 Number of children without parental care in public residential care (at the end of the year)": "NUMBER",
             "6.2.17 Number of children in infant homes (at the end of the year)": "NUMBER",
             "6.2.18 Rate of children in infant homes (at the end of the year, per 100,000 population aged 0-3)": "PCNT",
             "6.2.19 Number of children aged 0-2 years in infant homes (at the end of the year)": "NUMBER",
             "6.2.20 Number of children in general boarding schools - under the full state support (at the end of the year)": "NUMBER",
             "6.2.21 Number of children in non-public residential care (at the end of the year)": "NUMBER",
             "6.2.22 Children in non-public residential care by sex: boys (at the end of the year)": "NUMBER",
             "6.2.23 Children in non-public residential care by sex: girls (at the end of the year)": "NUMBER",
             "6.2.24 Children in non-public residential care by age: 0-2 years olds (at the end of the year)": "NUMBER",
             "6.2.25 Children in non-public residential care by age: 3-6 years olds (at the end of the year)": "NUMBER",
             "6.2.26 Children in non-public residential care by age: 7-17 years olds (at the end of the year)": "NUMBER",
             "6.2.27 Children in non-public residential care by age: 18 years and older (at the end of the year)": "NUMBER",
             "6.2.28 Number of children who left public residential care - including those transferred to another institution (during the year)": "NUMBER",
             "6.2.29 Children who left public residential care by reason: were returned to their parents /reintegrated in the family (during the year)": "NUMBER",
             "6.2.30 Children who left public residential care by reason: were placed into family type care (during the year)": "NUMBER",
             "6.2.31 Children who left public residential care by reason: were adopted (during the year)": "NUMBER",
             "6.2.32 Children who left public residential care by reason: started independent life (during the year)": "NUMBER",
             "6.2.33 Children who left public residential care by reason: transferred to another insitution (during the year)": "NUMBER",
             "6.2.34 Children who left public residential care by reason: died (during the year)": "NUMBER",
             "6.2.35 Children who left public residential care by reason: other (during the year)": "NUMBER",
             "6.3.1 Children in care of foster parents or guardians (at the end of the year)": "NUMBER",
             "6.3.2 Rate of children in care of foster parents or guardians (at the end of the year, per 100,000 population aged 0-17)": "PCNT",
             "6.3.3 Number of registered foster families (at the end of the year)": "NUMBER",
             "6.3.4 Total number of children who entered foster care (during the year)": "NUMBER",
             "6.3.5 Children who entered foster care by sex: boys (during the year)": "NUMBER",
             "6.3.6 Children who entered foster care by sex: girls (during the year)": "NUMBER",
             "6.3.7  Children who entered foster care by age: 0-2 years olds (during the year)": "NUMBER",
             "6.3.8 Children who entered foster care by age: 3-6 years olds (during the year)": "NUMBER",
             "6.3.9 Children who entered foster care by age: 7-17 years olds (during the year)": "NUMBER",
             "6.3.10 Total number of children who left foster care (during the year)": "NUMBER",
             "6.3.11 Children who left foster care by reason: were returned to their biological parents (during the year)": "NUMBER",
             "6.3.12 Children who left foster care by reason: entered child care/educational institutions (during the year)": "NUMBER",
             "6.3.13 Children who left foster care by reason for leaving: started independent life (during the year)": "NUMBER",
             "6.3.14 Children who left foster care by reason: other (during the year)": "NUMBER",
             "6.3.15 Children who left foster care by reason: placed into other foster family (during the year)": "NUMBER",
             "6.3.16 Total number of children cared for by foster parents (at the end of the year)": "NUMBER",
             "6.3.17 Children in foster care by age: 0-2 years old (at the end of the year)": "NUMBER",
             "6.3.18 Children in foster care by age: 3-6 years old (at the end of the year)": "NUMBER",
             "6.3.19 Children in foster care by age: 7-17 years old (at the end of the year)": "NUMBER",
             "6.3.20 Children in foster care by age: 18 years and older (at the end of the year)": "NUMBER",
             "6.3.21 Number of children with disabilities in foster care (at the end of the year)": "NUMBER",
             "6.3.22 Total number of children who entered guardian care (during the year)": "NUMBER",
             "6.3.23 Children who entered guardian care by sex: boys (during the year)": "NUMBER",
             "6.3.24 Children who entered guardian care by sex: girls (during the year)": "NUMBER",
             "6.3.25 Children who entered guardian care by age: 0-2 years olds (during the year)": "NUMBER",
             "6.3.26 Children who entered guardian care by age: 3-6 years olds (during the year)": "NUMBER",
             "6.3.27 Children who entered guardian care by age: 7-17 years olds (during the year)": "NUMBER",
             "6.3.28 Total number of children who left guardian care (during the year)": "NUMBER",
             "6.3.29 Children who left guardian care by reason: returned to their biological parents (during the year)": "NUMBER",
             "6.3.30 Children who left guardian care by reason: entered child care/educational institutions (during the year)": "NUMBER",
             "6.3.31 Children who left guardian care by reason: started independent life (during the year)": "NUMBER",
             "6.3.32 Children who left guardian care by reason: other (during the year)": "NUMBER",
             "6.3.33 Total number of children cared for by guardians (at the end of the year)": "NUMBER",
             "6.3.34 Number of children with disabilities in guardian care (at the end of the year)": "NUMBER",
             "6.4.1 Total number of adoptions - including intercountry adoptions (during the year)": "NUMBER",
             "6.4.2 Gross adoption rate (per 100,000 average population aged  0-3)": "PCNT",
             "6.4.3 Adopted children by age: 0-2 years old (during the year)": "NUMBER",
             "6.4.4 Adopted children by age: 3-6 years old (during the year)": "NUMBER",
             "6.4.5 Adopted children by age: 7-17 years old (during the year)": "NUMBER",
             "6.4.6 Number of adopted children with disabilities (during the year)": "NUMBER",
             "6.4.7 Total number of intercountry adoptions (during the year)": "NUMBER",
             "6.4.8 Intercountry adoption rate (per 100,000 average population aged  0-3)": "NUMBER",
             "6.4.9 Intercountry adoptions: number of adopted children with disabilities (during the year)": "NUMBER",
             "6.4.10 Total number of children available for adoption (at the end of the year)": "NUMBER",
             "6.4.11 Children available for adoption by age: 0-2 years old (at the end of the year)": "NUMBER",
             "6.4.12 Children available for adoption by age: 3-6 years old (at the end of the year)": "NUMBER",
             "6.4.13 Children available for adoption by age: 7-9 years old (at the end of the year)": "NUMBER",
             "6.4.14 Children available for adoption by age: 10-17 years old (at the end of the year)": "NUMBER",
             "6.4.15 Number of children with disabilities available for adoption (at the end of the year)": "NUMBER",

             "8.1.1 Total Social Protection expenditure as % of GDP": "PCNT",
             "8.1.2 Expenditure on cash social benefits as % of GDP": "PCNT",
             "8.1.3 Expenditure on social benefits in kind as % of GDP": "PCNT",
             "8.1.4 Expenditure on social benefits under Family/Children function as % of GDP": "PCNT",
             "8.1.5 Expenditure on cash social benefits under Family/Children function as % of GDP": "PCNT",
             "8.1.6 Expenditure on social benefits in kind under Family/Children function as % of GDP": "PCNT",
             "8.1.7 Expenditure on means-tested social protection benefits as % of total social protection expenditure ": "PCNT",
             "8.1.8 Expenditure on cash social benefits as % of total social protection expenditure": "PCNT",
             "8.1.9 Expenditure on social benefits in kind as % of total social protection expenditure": "PCNT",
             "8.1.10 Expenditure on social benefits under Family/Children function as % of total social protection expenditure ": "PCNT",
             "8.1.11 Social benefits in kind under family/children function as % of total social benefits in kind  ": "PCNT",
             "8.1.12 Expenditure on Family allowances as % of total cash expenditure under family/children function ": "PCNT",
             "8.1.13 Expenditure on Income compensation during maternity as % of total cash expenditure under family/children function ": "PCNT",
             "8.1.14  Expenditure on Parental leave as % of total cash expenditure under family/children function ": "PCNT",
             "8.1.15 Expenditure on Birth grant as % of total cash expenditure under family/children function ": "PCNT",
             "8.1.16 Expenditure on other family cash social benefits as % of total cash expenditure under family/children function ": "PCNT",

             "8.2.1 Total number of children receiving monthly family allowances": "NUMBER",
             "8.2.2 Number of children receiving means-tested family allowances": "NUMBER",
             "8.2.3 Number of children of single-parent families receiving specific allowances": "NUMBER",
             "8.2.4 Number of children receiving other child allowances": "NUMBER",
             "8.2.5 Number of children for whom foster/guardian allowance is received": "NUMBER",
             "8.2.6 Total number of families receiving monthly family allowances": "NUMBER",
             "8.2.7 Number of single-parent families receiving specific allowances": "NUMBER",
             "8.2.8 Number of families receiving other child allowances": "NUMBER",
             "8.2.9 Total number of families receiving foster/guardian allowance": "NUMBER",
             "8.2.10 Daily average number of women receiving maternity leave in the period": "NUMBER",
             "8.2.11 Total number of women receiving birth grant in the year ": "NUMBER",
             "8.2.12 Daily average number of parents on parental leave": "NUMBER",

         }},
    "country": {
        "albania": "ALB",
        "armenia": "ARM",
        "azerbaijan": "AZE",
        "belarus": "BLR",
        "bosnia and herzegovina": "BIH",
        "bulgaria": "BGR",
        "croatia": "HRV",
        "czech republic": "CZE",
        "estonia": "EST",
        "georgia": "GEO",
        "hungary": "HUN",
        "kazakhstan": "KAZ",
        "kyrgyzstan": "KGZ",
        "latvia": "LVA",
        "lithuania": "LTU",
        "moldova": "MDA",
        "montenegro": "MNE",
        "poland": "POL",
        "romania": "ROU",
        "russian federation": "RUS",
        "serbia": "SRB",
        "slovakia": "SVK",
        "slovenia": "SVN",
        "tajikistan": "TJK",
        "the former yugoslav republic of macedonia": "MKD",
        "turkmenistan": "TKM",
        "ukraine": "UKR",
        "uzbekistan": "UZB",
    },
    "indicator": {
        "6.1.1 Total number of children left without parental care (during the year)": "PT_CHLD_NO_PARENTAL_CARE",
        "6.1.2 Rate of children left without parental care (during the year, per 100,000 average population aged 0-17)": "PT_CHLD_NO_PARENTAL_CARE_RATE",
        "6.1.3 Children left without parental care by sex: boys (during the year)": "PT_CHLD_NO_PARENTAL_CARE",
        "6.1.4 Children left without parental care by sex: girls (during the year)": "PT_CHLD_NO_PARENTAL_CARE",
        "6.1.5 Children left without parental care by age: 0-2 year olds (during the year)": "PT_CHLD_NO_PARENTAL_CARE",
        "6.1.6 Children left without parental care by age: 3-6 year olds (during the year)": "PT_CHLD_NO_PARENTAL_CARE",
        "6.1.7 Children left without parental care by age: 7-17 year olds (during the year)": "PT_CHLD_NO_PARENTAL_CARE",
        "6.1.8 Children with disabilities left without parental care (during the year)": "PT_CHLD_NO_PARENTAL_CARE_DISABILITY",
        "6.1.9 Children with disabilities left without parental care by sex: boys (during the year)": "PT_CHLD_NO_PARENTAL_CARE_DISABILITY",
        "6.1.10 Children with disabilities left without parental care by sex: girls (during the year)": "PT_CHLD_NO_PARENTAL_CARE_DISABILITY",
        "6.1.11 Children left without parental care by cause: orphan children (during the year)": "PT_CHLD_NO_PARENTAL_CARE_ORPHAN",
        "6.1.12 Children left without parental care by cause: deprivation of parental rights (during the year)": "PT_CHLD_NO_PARENTAL_CARE_PARRIGHTS",
        "6.1.13 Children left without parental care by cause: abandonment or relinquishment by the parents (during the year)": "PT_CHLD_NO_PARENTAL_CARE_ABANDON",
        "6.1.14 Children left without parental care by cause: parents temporary unable/ not in a position to care for the child (during the year)": "PT_CHLD_NO_PARENTAL_CARE_UNABLE",
        "6.1.15 Children left without parental care by cause: other (during the year)": "PT_CHLD_NO_PARENTAL_CARE_OTHER",
        "6.1.16 Children left without parental care placed into care (during the year)": "PT_CHLD_NO_PARENTAL_CARE_INCARE",
        "6.1.17 Children left without parental care by type of placement: in residential care (during the year)": "PT_CHLD_NO_PARENTAL_CARE_INRESIDENTIAL",
        "6.1.18 Children left without parental care by type of placement: entered educational institution (during the year)": "PT_CHLD_NO_PARENTAL_CARE_ININSTITUTE",
        "6.1.19 Children left without parental care by type of placement: entered foster care (during the year)": "PT_CHLD_NO_PARENTAL_CARE_INFOSTER",
        "6.1.20 Children left without parental care by type of placement: entered guardian care (during the year)": "PT_CHLD_NO_PARENTAL_CARE_INGUARDIAN",
        "6.1.21 Children left without parental care by type of placement: were adopted (during the year)": "PT_CHLD_NO_PARENTAL_CARE_ADOPTED",
        "6.1.22 Total number of children in formal care (at the end of the year)": "PT_CHLD_INFORMALCARE",
        "6.1.23 Rate of children in formal care (at the end of the year, per 100,000 population aged 0-17)": "PT_CHLD_INFORMALCARE_RATE",
        "6.2.1 Total number of children in residential care (at the end of the year)": "PT_CHLD_INRESIDENTIAL",
        "6.2.2 Rate of children in residential care (at the end of the year, per 100,000 population aged  0-17)": "PT_CHLD_INRESIDENTIAL_RATE",
        "6.2.3 Children in residential care by sex: boys (at the end of the year)": "PT_CHLD_INRESIDENTIAL",
        "6.2.4 Children in residential care by sex: girls (at the end of the year)": "PT_CHLD_INRESIDENTIAL",
        "6.2.5 Children in residential care by age: 0-2 years olds (at the end of the year)": "PT_CHLD_INRESIDENTIAL",
        "6.2.6 Children in residential care by age: 3-6 years olds (at the end of the year)": "PT_CHLD_INRESIDENTIAL",
        "6.2.7 Children in residential care by age: 7-17 years olds (at the end of the year)": "PT_CHLD_INRESIDENTIAL",
        "6.2.8 Children in residential care by age: 18 years and older (at the end of the year)": "PT_CHLD_INRESIDENTIAL",
        "6.2.9 Number of children with disabilities in public residential care - all types of institutions (at the end of the year)": "PT_CHLD_DISAB_PUBLIC",
        "6.2.10 Children with disabilities in public residential care by sex: boys (at the end of the year)": "PT_CHLD_DISAB_PUBLIC",
        "6.2.11 Children with disabilities in public residential care by sex: girls (at the end of the year)": "PT_CHLD_DISAB_PUBLIC",
        "6.2.12 Children with disability in public residential care by age: 0-2 years olds (at the end of the year)": "PT_CHLD_DISAB_PUBLIC",
        "6.2.13 Children with disability in public residential care by age: 3-6 years olds (at the end of the year)": "PT_CHLD_DISAB_PUBLIC",
        "6.2.14 Children with disability in public residential care by age: 7-17 years olds (at the end of the year)": "PT_CHLD_DISAB_PUBLIC",
        "6.2.15 Children with disability in public residential care by age: 18 years old and elder (at the end of the year)": "PT_CHLD_DISAB_PUBLIC",
        "6.2.16 Number of children without parental care in public residential care (at the end of the year)": "PT_CHLD_NO_PARENTAL_CARE_PUBLIC",
        "6.2.17 Number of children in infant homes (at the end of the year)": "PT_CHLD_ININFANTHOME",
        "6.2.18 Rate of children in infant homes (at the end of the year, per 100,000 population aged 0-3)": "PT_CHLD_ININFANTHOME_RATE",
        "6.2.19 Number of children aged 0-2 years in infant homes (at the end of the year)": "PT_CHLD_ININFANTHOME",
        "6.2.20 Number of children in general boarding schools - under the full state support (at the end of the year)": "PT_CHLD_INBOARDING",
        "6.2.21 Number of children in non-public residential care (at the end of the year)": "PT_CHLD_NONPUBLIC",
        "6.2.22 Children in non-public residential care by sex: boys (at the end of the year)": "PT_CHLD_NONPUBLIC",
        "6.2.23 Children in non-public residential care by sex: girls (at the end of the year)": "PT_CHLD_NONPUBLIC",
        "6.2.24 Children in non-public residential care by age: 0-2 years olds (at the end of the year)": "PT_CHLD_NONPUBLIC",
        "6.2.25 Children in non-public residential care by age: 3-6 years olds (at the end of the year)": "PT_CHLD_NONPUBLIC",
        "6.2.26 Children in non-public residential care by age: 7-17 years olds (at the end of the year)": "PT_CHLD_NONPUBLIC",
        "6.2.27 Children in non-public residential care by age: 18 years and older (at the end of the year)": "PT_CHLD_NONPUBLIC",
        "6.2.28 Number of children who left public residential care - including those transferred to another institution (during the year)": "PT_CHLD_LEFTRESCARE",
        "6.2.29 Children who left public residential care by reason: were returned to their parents /reintegrated in the family (during the year)": "PT_CHLD_LEFTRESCARE_RETURNED",
        "6.2.30 Children who left public residential care by reason: were placed into family type care (during the year)": "PT_CHLD_LEFTRESCARE_INFAMILY",
        "6.2.31 Children who left public residential care by reason: were adopted (during the year)": "PT_CHLD_LEFTRESCARE_ADOPTED",
        "6.2.32 Children who left public residential care by reason: started independent life (during the year)": "PT_CHLD_LEFTRESCARE_INDEPENDENT",
        "6.2.33 Children who left public residential care by reason: transferred to another insitution (during the year)": "PT_CHLD_LEFTRESCARE_TRANSFERED",
        "6.2.34 Children who left public residential care by reason: died (during the year)": "PT_CHLD_LEFTRESCARE_DIED",
        "6.2.35 Children who left public residential care by reason: other (during the year)": "PT_CHLD_LEFTRESCARE_OTHER",
        "6.3.1 Children in care of foster parents or guardians (at the end of the year)": "PT_CHLD_INCARE_FOSTER",
        "6.3.2 Rate of children in care of foster parents or guardians (at the end of the year, per 100,000 population aged 0-17)": "PT_CHLD_INCARE_FOSTER_RATE",
        "6.3.3 Number of registered foster families (at the end of the year)": "PT_CHLD_REGISTERED_FORSTER",
        "6.3.4 Total number of children who entered foster care (during the year)": "PT_CHLD_ENTEREDFOSTER",
        "6.3.5 Children who entered foster care by sex: boys (during the year)": "PT_CHLD_ENTEREDFOSTER",
        "6.3.6 Children who entered foster care by sex: girls (during the year)": "PT_CHLD_ENTEREDFOSTER",
        "6.3.7  Children who entered foster care by age: 0-2 years olds (during the year)": "PT_CHLD_ENTEREDFOSTER",
        "6.3.8 Children who entered foster care by age: 3-6 years olds (during the year)": "PT_CHLD_ENTEREDFOSTER",
        "6.3.9 Children who entered foster care by age: 7-17 years olds (during the year)": "PT_CHLD_ENTEREDFOSTER",
        "6.3.10 Total number of children who left foster care (during the year)": "PT_CHLD_LEFTFOSTER",
        "6.3.11 Children who left foster care by reason: were returned to their biological parents (during the year)": "PT_CHLD_LEFTFOSTER_RETURNED",
        "6.3.12 Children who left foster care by reason: entered child care/educational institutions (during the year)": "PT_CHLD_LEFTFOSTER_EDUINSTITUTION",
        "6.3.13 Children who left foster care by reason for leaving: started independent life (during the year)": "PT_CHLD_LEFTFOSTER_INDEPENDENT",
        "6.3.14 Children who left foster care by reason: other (during the year)": "PT_CHLD_LEFTFOSTER_OTHER",
        "6.3.15 Children who left foster care by reason: placed into other foster family (during the year)": "PT_CHLD_LEFTFOSTER_INFAMILY",
        "6.3.16 Total number of children cared for by foster parents (at the end of the year)": "PT_CHLD_CARED_BY_FOSTER",
        "6.3.17 Children in foster care by age: 0-2 years old (at the end of the year)": "PT_CHLD_CARED_BY_FOSTER",
        "6.3.18 Children in foster care by age: 3-6 years old (at the end of the year)": "PT_CHLD_CARED_BY_FOSTER",
        "6.3.19 Children in foster care by age: 7-17 years old (at the end of the year)": "PT_CHLD_CARED_BY_FOSTER",
        "6.3.20 Children in foster care by age: 18 years and older (at the end of the year)": "PT_CHLD_CARED_BY_FOSTER",
        "6.3.21 Number of children with disabilities in foster care (at the end of the year)": "PT_CHLD_DISAB_FOSTER",
        "6.3.22 Total number of children who entered guardian care (during the year)": "PT_CHLD_GUARDIAN",
        "6.3.23 Children who entered guardian care by sex: boys (during the year)": "PT_CHLD_GUARDIAN",
        "6.3.24 Children who entered guardian care by sex: girls (during the year)": "PT_CHLD_GUARDIAN",
        "6.3.25 Children who entered guardian care by age: 0-2 years olds (during the year)": "PT_CHLD_GUARDIAN",
        "6.3.26 Children who entered guardian care by age: 3-6 years olds (during the year)": "PT_CHLD_GUARDIAN",
        "6.3.27 Children who entered guardian care by age: 7-17 years olds (during the year)": "PT_CHLD_GUARDIAN",
        "6.3.28 Total number of children who left guardian care (during the year)": "PT_CHLD_GUARDIAN_LEFT",
        "6.3.29 Children who left guardian care by reason: returned to their biological parents (during the year)": "PT_CHLD_GUARDIAN_LEFT_RETURNED",
        "6.3.30 Children who left guardian care by reason: entered child care/educational institutions (during the year)": "PT_CHLD_GUARDIAN_LEFT_INSTITUTION",
        "6.3.31 Children who left guardian care by reason: started independent life (during the year)": "PT_CHLD_GUARDIAN_LEFT_INDEPENDET",
        "6.3.32 Children who left guardian care by reason: other (during the year)": "PT_CHLD_GUARDIAN_LEFT_OTHER",
        "6.3.33 Total number of children cared for by guardians (at the end of the year)": "PT_CHLD_CARED_GUARDIAN",
        "6.3.34 Number of children with disabilities in guardian care (at the end of the year)": "PT_CHLD_DISAB_CARED_GUARDIAN",
        "6.4.1 Total number of adoptions - including intercountry adoptions (during the year)": "PT_CHLD_ADOPTION",
        "6.4.2 Gross adoption rate (per 100,000 average population aged  0-3)": "PT_CHLD_ADOPTION_RATE",
        "6.4.3 Adopted children by age: 0-2 years old (during the year)": "PT_CHLD_ADOPTION",
        "6.4.4 Adopted children by age: 3-6 years old (during the year)": "PT_CHLD_ADOPTION",
        "6.4.5 Adopted children by age: 7-17 years old (during the year)": "PT_CHLD_ADOPTION",
        "6.4.6 Number of adopted children with disabilities (during the year)": "PT_CHLD_ADOPTION_DISAB",
        "6.4.7 Total number of intercountry adoptions (during the year)": "PT_CHLD_ADOPTION_INTERCOUNTRY",
        "6.4.8 Intercountry adoption rate (per 100,000 average population aged  0-3)": "PT_CHLD_ADOPTION_INTERCOUNTRY_RATE",
        "6.4.9 Intercountry adoptions: number of adopted children with disabilities (during the year)": "PT_CHLD_ADOPTION_INTER_COUNTRY_DISAB",
        "6.4.10 Total number of children available for adoption (at the end of the year)": "PT_CHLD_ADOPTION_AVAILABLE",
        "6.4.11 Children available for adoption by age: 0-2 years old (at the end of the year)": "PT_CHLD_ADOPTION_AVAILABLE",
        "6.4.12 Children available for adoption by age: 3-6 years old (at the end of the year)": "PT_CHLD_ADOPTION_AVAILABLE",
        "6.4.13 Children available for adoption by age: 7-9 years old (at the end of the year)": "PT_CHLD_ADOPTION_AVAILABLE",
        "6.4.14 Children available for adoption by age: 10-17 years old (at the end of the year)": "PT_CHLD_ADOPTION_AVAILABLE",
        "6.4.15 Number of children with disabilities available for adoption (at the end of the year)": "PT_CHLD_ADOPTION_AVAILABLE_DISAB",

        "8.1.1 Total Social Protection expenditure as % of GDP": "SP_TOT",
        "8.1.2 Expenditure on cash social benefits as % of GDP": "SP_SOC_BEN_CASH",
        "8.1.3 Expenditure on social benefits in kind as % of GDP": "SP_BEN_KIND",
        "8.1.4 Expenditure on social benefits under Family/Children function as % of GDP": "SP_BEN_FAMILY",
        "8.1.5 Expenditure on cash social benefits under Family/Children function as % of GDP": "SP_BEN_FAMILY_CASH",
        "8.1.6 Expenditure on social benefits in kind under Family/Children function as % of GDP": "SP_BEN_FAMILY_KIND",
        "8.1.7 Expenditure on means-tested social protection benefits as % of total social protection expenditure ": "SP_MEANS_TESTED",
        "8.1.8 Expenditure on cash social benefits as % of total social protection expenditure": "SP_CASH_EXP",
        "8.1.9 Expenditure on social benefits in kind as % of total social protection expenditure": "SP_KIND_EXP",
        "8.1.10 Expenditure on social benefits under Family/Children function as % of total social protection expenditure ": "SP_BEN_FAMILY_EXP",
        "8.1.11 Social benefits in kind under family/children function as % of total social benefits in kind  ": "SP_KIND_FAMILY_KIND",
        "8.1.12 Expenditure on Family allowances as % of total cash expenditure under family/children function ": "SP_FAMILY_ALLOW",
        "8.1.13 Expenditure on Income compensation during maternity as % of total cash expenditure under family/children function ": "SP_IMCOME_MATERNITY",
        "8.1.14  Expenditure on Parental leave as % of total cash expenditure under family/children function ": "SP_PARENTAL_LEAVE",
        "8.1.15 Expenditure on Birth grant as % of total cash expenditure under family/children function ": "SP_BITRH_GRANT",
        "8.1.16 Expenditure on other family cash social benefits as % of total cash expenditure under family/children function ": "SP_OTHER_FAMILY",

        "8.2.1 Total number of children receiving monthly family allowances": "SP_ALLOW_CHILD",
        "8.2.2 Number of children receiving means-tested family allowances": "SP_ALLOW_MEANSTEST",
        "8.2.3 Number of children of single-parent families receiving specific allowances": "SP_ALLOW_SINGLE",
        "8.2.4 Number of children receiving other child allowances": "SP_ALLOW_OTHER_CHILD",
        "8.2.5 Number of children for whom foster/guardian allowance is received": "SP_ALLOW_FOSGUARD",
        "8.2.6 Total number of families receiving monthly family allowances": "SP_ALLOW_FAMILY",
        "8.2.7 Number of single-parent families receiving specific allowances": "SP_ALLOW_FAMILY_SINGLE",
        "8.2.8 Number of families receiving other child allowances": "SP_ALLOW_FAMILY_OTHER_CHILD",
        "8.2.9 Total number of families receiving foster/guardian allowance": "SP_ALLOW_FAMILY_FOSSINGLE",
        "8.2.10 Daily average number of women receiving maternity leave in the period": "SP_MOTHLEAVE_DAYSAVG",
        "8.2.11 Total number of women receiving birth grant in the year ": "SP_WOM_BIRTHGRANT",
        "8.2.12 Daily average number of parents on parental leave": "SP_PATLEAVE_DAYSAVG",

    }
}

# SOC_PROT_GOV_INT = "Z:\\TransMonee\\01_getData\\from_site\\8.1-Government-interventions.xlsx"
# task = tasks.transmonee_files.Transmonee_data.TransmoneeData(SOC_PROT_GOV_INT)
# tm_data = task.getdata(struct.getCSVColumns(), colMap_tm, const, codeMap=transmoneeCodeMap)
# destination2 = destination2.append(tm_data)

# SOC_PROT_FAMILYSUPPORT = "Z:\\TransMonee\\01_getData\\from_site\\8.2-Family-support.xlsx"
# task = tasks.transmonee_files.Transmonee_data.TransmoneeData(SOC_PROT_FAMILYSUPPORT)
# tm_data = task.getdata(struct.getCSVColumns(), colMap_tm, const, codeMap=transmoneeCodeMap)
# destination2 = destination2.append(tm_data)


CH_PROT_NO_PARENTAL_CARE = "Z:\\TransMonee\\01_getData\\from_site\\6.1-Children-left-without-parental-care-during-the-reference-year.xlsx"
task = tasks.transmonee_files.Transmonee_data.TransmoneeData(CH_PROT_NO_PARENTAL_CARE)
tm_data = task.getdata(struct.getCSVColumns(), colMap_tm, const, codeMap=transmoneeCodeMap)
destination2 = destination2.append(tm_data)

Ch_PROT_RES_CARE = "Z:\\TransMonee\\01_getData\\from_site\\6.2-Children-in-residential-care.xlsx"
task = tasks.transmonee_files.Transmonee_data.TransmoneeData(Ch_PROT_RES_CARE)
tm_data = task.getdata(struct.getCSVColumns(), colMap_tm, const, codeMap=transmoneeCodeMap)
destination2 = destination2.append(tm_data)

CH_PROT_FAMILY_CARE = "Z:\\TransMonee\\01_getData\\from_site\\6.3-Children-in-family-type-care-.xlsx"
task = tasks.transmonee_files.Transmonee_data.TransmoneeData(CH_PROT_FAMILY_CARE)
tm_data = task.getdata(struct.getCSVColumns(), colMap_tm, const, codeMap=transmoneeCodeMap)
destination2 = destination2.append(tm_data)

CH_PROT_ADOPTIONS = "Z:\\TransMonee\\01_getData\\from_site\\6.4-Adoptions.xlsx"
task = tasks.transmonee_files.Transmonee_data.TransmoneeData(CH_PROT_ADOPTIONS)
tm_data = task.getdata(struct.getCSVColumns(), colMap_tm, const, codeMap=transmoneeCodeMap)
destination2 = destination2.append(tm_data)

destination2.to_csv(os.path.join(DIR_output, "tm.csv"), sep=",", header=True, encoding="utf-8", index=False)
