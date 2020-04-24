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

    def __init__(self, source_config_file, colMap, codeMap):
        datalist = fileUtils.CsvUtils.readDictionaryFromCSV(source_config_file)
        self._colMap = colMap
        self._codeMap = codeMap
        self._toProcess = []
        for l in datalist:
            toAppend = {"dataflowId": [l['agency'], l['df'], l['ver']],
                        "dq": l["dq"],
                        "params": UNESCO.query_params,
                        "tmp_file": l["tmp_file"],
                        "const": {

                        }
                        }
            for k in l:
                if k.startswith("c_"):
                    toAppend["const"][k.replace("c_","")]=l[k]
            self._toProcess.append(toAppend)

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

    def getdata(self, workingPath, cols, filterFunction=None):
        ret = pd.DataFrame(columns=cols, dtype=str)
        for p in self._toProcess:
            toAdd = self._process(os.path.join(workingPath, p['tmp_file']), self._codeMap, self._colMap,
                                  p['const'],
                                  filterFun=filterFunction)
            self.append = ret.append(toAdd)
            ret = self.append
        return ret
