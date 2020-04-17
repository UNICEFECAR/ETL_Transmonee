import fileUtils.FileDownload

import os.path
import pandas as pd
import sdmx.sdmx_conversions
import processData.ColumnMapper as ColumnMapper

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data/spr_exp_gdp/.SPBENEFNOREROUTE..TOTAL.BA+BG?startPeriod=2000&endPeriod=2050
# http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data/spr_exp_gdp/.SPBENEFNOREROUTE..TOTAL.BA+BG?startPeriod=2000&endPeriod=2050

EUROSTAT_PARAMS = {"url": "http://ec.europa.eu/eurostat/SDMX/diss-web/rest/data",
                   "headers": {
                       "Accept": "application/vnd.sdmx.structurespecificdata+xml",
                   }}

DownloadParams_EUROSTAT_df_spr_exp_gdp = [
    {"dataflowId": "spr_exp_gdp",
     "dq": ".SPBENEFNOREROUTE..TOTAL+SICK.BA+BG+HR+ME+MK+RO+RS+TR",
     "params": {"startPeriod": "2000", "endPeriod": "2050"},
     "out": "ESTAT_01.xml"
     },
]

toProcess = [
    {
        "in": "ESTAT_01.xml",
        "const": {
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "SEX": "_T",
            "AGE": "_T",
            "UNIT_MEASURE": "PCNT",
            "OBS_FOOTNOTE": "",
            "DATA_SOURCE": "EUROSTAT",
            "UNIT_MULTIPLIER": "",
            "OBS_STATUS": ""
        },
        "filterOut": {
            "value": ""
        }
    },
]

colMap_EUROSTAT_spr_exp_gdp = {
    "Dataflow": {"type": "const", "value": "ECARO:TRANSMONEE(1.0)"},
    "REF_AREA": {"type": "col", "role": "dim", "value": "GEO"},
    "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "SPFUNC"},
    "SEX": {"type": "const", "role": "dim"},
    "AGE": {"type": "const", "role": "dim"},
    "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
    "OBS_VALUE": {"type": "col", "role": "obs", "value": "value"},
    "UNIT_MEASURE": {"type": "const", "role": "attrib"},
    "OBS_FOOTNOTE": {"type": "const", "role": "attrib", },
    "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
    "DATA_SOURCE": {"type": "const", "role": "attrib"},
    "UNIT_MULTIPLIER": {"type": "const", "role": "attrib"},
    "OBS_STATUS": {"type": "const", "role": "attrib"},

}

codeMap = {"GEO": {
    # "AL": "ALB",
    # "AM": "ARM",
    # "AZ": "AZE",
    "BA": "BIH",
    "BG": "BGR",
    # "BY": "BLR",
    # "GE": "GEO",
    "HR": "HRV",
    # "KG": "KGZ",
    # "KZ": "KAZ",
    # "MD": "MDA",
    "ME": "MNE",
    "MK": "MKD",
    "RO": "ROU",
    "RS": "SRB",
    # "TJ": "TJK",
    # "TM": "TKM",
    "TR": "TUR",
    # "UA": "UKR",
    # "UZ": "UZB",
},
    "SPFUNC": {
        "TOTAL": "SOCPROT_GDPPERC_TOTAL",
        "SICK": "SOCPROT_GDPPERC_SICK",
    }}


def download_data(outfile_path, skipIfExists=False, verb=0):
    for toDown in DownloadParams_EUROSTAT_df_spr_exp_gdp:
        outFilePath = os.path.join(outfile_path, toDown['out'])
        res = fileUtils.FileDownload.download_eurostat_sdmx_file(outFilePath, EUROSTAT_PARAMS['url'],
                                                                 toDown['dataflowId'],
                                                                 toDown["dq"],
                                                                 params=toDown["params"],
                                                                 headers=EUROSTAT_PARAMS['headers'],
                                                                 skipIfExists=skipIfExists)
        if verb >= 3:
            print(toDown['out'] + " " + res)


def _process(input_file, process, code_map, col_map, constants, check_for_dups=True):
    src = sdmx.sdmx_conversions.sdmx_xml2pandas(input_file)
    colMapper = ColumnMapper.ColumnMapper(col_map)

    if "filterOut" in process:
        for f in process["filterOut"]:
            src = src[src[f] != process["filterOut"][f]]

    if check_for_dups:
        duplicateWarning = colMapper.getDuplicates(src)
        if not duplicateWarning.empty:
            print(input_file + " will generate duplicates")

    for col in code_map:
        for m in code_map[col]:
            src[col].replace(m, code_map[col][m], inplace=True)

    return colMapper.mapDataframe(src, constants)


def getdata(workingPath, cols):
    ret = pd.DataFrame(columns=cols, dtype=str)
    for p in toProcess:
        toAdd = _process(os.path.join(workingPath, p['in']), p, codeMap, colMap_EUROSTAT_spr_exp_gdp, p['const'])
        ret = ret.append(toAdd)
    return ret
