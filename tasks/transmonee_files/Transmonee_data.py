import tasks.transmonee_files.parser as parser
import processData.ColumnMapper as ColumnMapper
import pandas as pd


class TransmoneeData:

    def __init__(self, source_file):
        self._source_file = source_file

    def _process(self, data, col_map, constants, codeMap=None):
        # print(data.head())
        colMapper = ColumnMapper.ColumnMapper(col_map)

        if codeMap is not None:
            for col in codeMap:

                if "depends" in codeMap[col]:
                    for m in codeMap[col]['map']:
                        sourceCol = codeMap[col]["depends"]

                        mask = data[sourceCol] == m


                        data[col][mask] = codeMap[col]["map"][m]



                else:
                    for m in codeMap[col]:
                        data[col].replace(m, codeMap[col][m], inplace=True)

        mapped = colMapper.mapDataframe(data, constants)
        return mapped

    def getdata(self, cols, columnMap, constVals, filterFunction=None, codeMap=None):
        ret = pd.DataFrame(columns=cols, dtype=str)
        srcdata = parser.parse_transmonee_file(self._source_file)
        srcdata = self._process(srcdata, columnMap, constVals, codeMap)
        ret = ret.append(srcdata)
        return ret
