class ColumnMapper:

    def __init__(self, columnMap):
        self.columnMap = columnMap

    def _mapDFRow(self, row, const):
        ret = {}
        for c in self.columnMap:
            if (self.columnMap[c]["type"] == "const"):
                ret[c] = const[c]
            elif (self.columnMap[c]["type"] == "col"):
                ret[c] = row[self.columnMap[c]["value"]]
        return ret

    def mapDataframe(self, dataframe, constants):
        ret = []

        for r in range(0, len(dataframe)):
            ret.append(self._mapDFRow(dataframe.iloc[r], constants))

        return ret

    def getDimCols(self):
        cols = []
        for c in self.columnMap:
            if "role" in self.columnMap[c] and self.columnMap[c]['type'] == "col":
                if self.columnMap[c]['role'] == "dim" or self.columnMap[c]['role'] == "time":
                    cols.append(self.columnMap[c]['value'])
        return cols

    def getDuplicates(self, dataframe):
        dimCols=self.getDimCols()
        return dataframe[dataframe.duplicated(subset=dimCols, keep=False)]
