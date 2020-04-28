import tasks.transmonee_files.parser as parser
import processData.ColumnMapper as ColumnMapper
import pandas as pd


class TransmoneeData:

    # def __init__(self, source_file, destination_cols):
    #     self._source_file = source_file
    #     self._destination_cols = destination_cols

    # def _process(self, data, col_map, constants, codeMap=None):
    #     # print(data.head())
    #     colMapper = ColumnMapper.ColumnMapper(col_map)
    #
    #     mapped = colMapper.mapDataframe(data, constants)
    #     return mapped



    def readSource(self, source_file):
        data=parser.parse_transmonee_file(source_file)
        return data



    def map_codes(self, data, codemap):
        DEP = "depends"
        MAP = "map"
        if codemap is None:
            return data

        for col in codemap:
            if DEP in codemap[col]:
                for m in codemap[col][MAP]:
                    sourceCol = codemap[col][DEP]
                    mask = data[sourceCol] == m
                    data[col][mask] = codemap[col][MAP][m]
            else:
                for m in codemap[col]:
                    data[col].replace(m, codemap[col][m], inplace=True)

        return data

    def mapcols(self, data, colmap, consts):
        colmapper = ColumnMapper.ColumnMapper(colmap)
        return colmapper.mapDataframe(data, consts)

    # def getdata(self, cols, columnMap, constVals, filterFunction=None, codeMap=None):
    #     ret = pd.DataFrame(columns=cols, dtype=str)
    #     srcdata = parser.parse_transmonee_file(self._source_file)
    #     srcdata = self._process(srcdata, columnMap, constVals, codeMap)
    #     ret = ret.append(srcdata)
    #     return ret
