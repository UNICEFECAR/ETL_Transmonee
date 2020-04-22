import fileUtils.FileDownload
import fileUtils.CsvUtils

import os.path
import pandas as pd
import processData.ColumnMapper as ColumnMapper

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# https://api.uis.unesco.org/sdmx/data/UNESCO,SDG4,2.0/GER.PT.L01.._T..............AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ?startPeriod=2000&endPeriod=2050&format=sdmx-compact-2.0&locale=en&subscription-key=9d48382df9ad408ca538352a4186791b

UIS_API_KEY = "9d48382df9ad408ca538352a4186791b"  # Transmonne API Key

UIS_PARAMS = {"url": "https://api.uis.unesco.org/sdmx/data/",
              "headers": {
                  "Accept": "application/vnd.sdmx.data+csv;version=1.0.0",
                  "Accept-Encoding": "gzip",
              }}

query_params = {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"}

'''
     "filterOut": {
         "UNIT_MEASURE": "GPIA"
     }
     '''

datalist = fileUtils.CsvUtils.readDictionaryFromCSV("Z:\\TransMonee\\src\\EDU_UIS_SDG4_toDown.csv")
_toProcess_UIS_dfSDG4 = []
for l in datalist:
    _toProcess_UIS_dfSDG4.append(
        {"dataflowId": [l['agency'], l['df'], l['ver']],
         "dq": l["dq"],
         "params": query_params,
         "tmp_file": l["tmp_file"],
         "const": {
             "UNICEF_INDICATOR": l["UNICEF_INDICATOR"],
             "DATA_SOURCE": l["DATA_SOURCE"],
             "OBS_FOOTNOTE": l["OBS_FOOTNOTE"]
         }
         }
    )

_toProcess_UIS_dfEDU_NON_FINANCE = []
datalist = fileUtils.CsvUtils.readDictionaryFromCSV("Z:\\TransMonee\\src\\EDU_UIS_EDU_NON_FIN_toDown.csv")

for l in datalist:
    _toProcess_UIS_dfEDU_NON_FINANCE.append(
        {"dataflowId": [l['agency'], l['df'], l['ver']],
         "dq": l["dq"],
         "params": query_params,
         "tmp_file": l["tmp_file"],
         "const": {
             "UNICEF_INDICATOR": l["UNICEF_INDICATOR"],
             "DATA_SOURCE": l["DATA_SOURCE"],
             "OBS_FOOTNOTE": l["OBS_FOOTNOTE"]
         }
         }
    )


colMap_UNESCO = {
    "Dataflow": {"type": "const", "value": "ECARO:TRANSMONEE(1.0)"},
    "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
    "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
    "SEX": {"type": "col", "role": "dim", "value": "SEX"},
    "AGE": {"type": "col", "role": "dim", "value": "AGE"},
    "GRADE": {"type": "col", "role": "dim", "value": "GRADE"},
    "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
    "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
    "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
    "OBS_FOOTNOTE": {"type": "const", "role": "attrib", },
    "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
    "DATA_SOURCE": {"type": "const", "role": "attrib", "value": "UIS"},
    "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
    "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
}

codemap_UNESCO = {"REF_AREA": {
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
},
    "UNIT_MEASURE": {
        "PER": "PS",
        "PT": "PCNT",
    },
    "AGE": {
        "UNDER1_AGE": "M023",
    }
}


def download_data(outfile_path, skipIfExists=False, verb=0):
    for toDown in _toProcess_UIS_dfSDG4:
        outFilePath = os.path.join(outfile_path, toDown['tmp_file'])
        res = fileUtils.FileDownload.download_sdmx_file(outFilePath, UIS_PARAMS['url'], toDown['dataflowId'],
                                                        toDown["dq"],
                                                        params=toDown["params"], headers=UIS_PARAMS['headers'],
                                                        apikey=UIS_API_KEY, skipIfExists=skipIfExists)
        if verb >= 3:
            print(toDown['tmp_file'] + " " + res)

    for toDown in _toProcess_UIS_dfEDU_NON_FINANCE:
        outFilePath = os.path.join(outfile_path, toDown['tmp_file'])
        res = fileUtils.FileDownload.download_sdmx_file(outFilePath, UIS_PARAMS['url'], toDown['dataflowId'],
                                                        toDown["dq"],
                                                        params=toDown["params"], headers=UIS_PARAMS['headers'],
                                                        apikey=UIS_API_KEY, skipIfExists=skipIfExists)
        if verb >= 3:
            print(toDown['tmp_file'] + " " + res)


def _process(input_file, process, code_map, col_map, constants, check_for_dups=True):
    src = pd.read_csv(input_file, dtype=str)
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
    for p in _toProcess_UIS_dfSDG4:
        toAdd = _process(os.path.join(workingPath, p['tmp_file']), p, codemap_UNESCO, colMap_UNESCO, p['const'])
        ret = ret.append(toAdd)
    for p in _toProcess_UIS_dfEDU_NON_FINANCE:
        toAdd = _process(os.path.join(workingPath, p['tmp_file']), p, codemap_UNESCO, colMap_UNESCO, p['const'])
        ret = ret.append(toAdd)
    return ret
