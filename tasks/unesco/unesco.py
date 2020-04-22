import fileUtils.FileDownload
import fileUtils.CsvUtils

import os.path
import pandas as pd
import processData.ColumnMapper as ColumnMapper

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# https://api.uis.unesco.org/sdmx/data/UNESCO,SDG4,2.0/GER.PT.L01.._T..............AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ?startPeriod=2000&endPeriod=2050&format=sdmx-compact-2.0&locale=en&subscription-key=9d48382df9ad408ca538352a4186791b
class UNESCO:
    UIS_API_KEY = "9d48382df9ad408ca538352a4186791b"  # Transmonne API Key

    UIS_PARAMS = {"url": "https://api.uis.unesco.org/sdmx/data/",
                  "headers": {
                      "Accept": "application/vnd.sdmx.data+csv;version=1.0.0",
                      "Accept-Encoding": "gzip",
                  }}

    query_params = {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"}

    colMap_UNESCO = {
        "Dataflow": {"type": "const", "value": "ECARO:TRANSMONEE(1.0)"},
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
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
        },
        "LOCATION": {
            "RUR": "R",
            "URB": "U",
            "_Z": "_T",
            "_T": "_T"
        }
    }

    def __init__(self, source_config_file):
        datalist = fileUtils.CsvUtils.readDictionaryFromCSV(source_config_file)
        self._toProcess = []
        for l in datalist:
            self._toProcess.append(
                {"dataflowId": [l['agency'], l['df'], l['ver']],
                 "dq": l["dq"],
                 "params": UNESCO.query_params,
                 "tmp_file": l["tmp_file"],
                 "const": {
                     "UNICEF_INDICATOR": l["UNICEF_INDICATOR"],
                     "DATA_SOURCE": l["DATA_SOURCE"],
                     "OBS_FOOTNOTE": l["OBS_FOOTNOTE"]
                 }
                 }
            )

    def download_data(self, outfile_path, skipIfExists=False, verb=0):
        for toDown in self._toProcess:
            outFilePath = os.path.join(outfile_path, toDown['tmp_file'])
            res = fileUtils.FileDownload.download_sdmx_file(outFilePath, UNESCO.UIS_PARAMS['url'], toDown['dataflowId'],
                                                            toDown["dq"],
                                                            params=toDown["params"],
                                                            headers=UNESCO.UIS_PARAMS['headers'],
                                                            apikey=UNESCO.UIS_API_KEY, skipIfExists=skipIfExists)
            if verb >= 3:
                print(toDown['tmp_file'] + " " + res)

    def _process(self, input_file, code_map, col_map, constants, filterFun=None, check_for_dups=True):
        src = pd.read_csv(input_file, dtype=str)
        colMapper = ColumnMapper.ColumnMapper(col_map)

        if filterFun is not None:
            src = filterFun(src)

        if check_for_dups:
            duplicateWarning = colMapper.getDuplicates(src)

            if not duplicateWarning.empty:
                print(input_file + " will generate duplicates")
        for col in code_map:
            for m in code_map[col]:
                src[col].replace(m, code_map[col][m], inplace=True)

        return colMapper.mapDataframe(src, constants)

    # def applySDG4Filters(df):
    #     # just keep the _T as socioeconomic background
    #     ret = df[df["SE_BKGRD"].str.contains("_T") | df["SE_BKGRD"].str.contains("_Z")]
    #     # just keep the _T as Immigration status
    #     ret = ret[ret["IMM_STATUS"].str.contains("_T") | ret["IMM_STATUS"].str.contains("_Z")]
    #
    #     return ret

    def getdata(self, workingPath, cols, filterFunction=None):
        ret = pd.DataFrame(columns=cols, dtype=str)
        for p in self._toProcess:
            toAdd = self._process(os.path.join(workingPath, p['tmp_file']), UNESCO.codemap_UNESCO, UNESCO.colMap_UNESCO, p['const'],
                             filterFun=filterFunction)
            self.append = ret.append(toAdd)
            ret = self.append
        return ret
