class ColumnMapper:
    '''
    Maps columns between
    '''
    def __init__(self, columnMap):
        self.columnMap = columnMap

    def _mapDFRow(self, row, const):
        '''
        Maps a row
        :param row: the row to map
        :param const: the constants
        :return: An dictionary with the mappings
        '''
        ret = {}
        for c in self.columnMap:
            if (self.columnMap[c]["type"] == "const" and c in const):
                ret[c] = const[c]
            elif (self.columnMap[c]["type"] == "col"):
                ret[c] = row[self.columnMap[c]["value"]]
        return ret

    def mapDataframe(self, dataframe, constants):
        '''
        Mpas the columns starting from a dataframe
        :param dataframe: The dataframe to map
        :param constants: the constant columns
        :return: The mapped columns
        '''
        ret = []

        for r in range(0, len(dataframe)):
            ret.append(self._mapDFRow(dataframe.iloc[r], constants))

        return ret

    def getDimCols(self):
        '''
        Gets the list of columns that are marked as Dimensions in the DSD
        :return: A list with just the dimensions
        '''
        cols = []
        for c in self.columnMap:
            if "role" in self.columnMap[c] and self.columnMap[c]['type'] == "col":
                if self.columnMap[c]['role'] == "dim" or self.columnMap[c]['role'] == "time":
                    cols.append(self.columnMap[c]['value'])
        return cols

    def getDuplicates(self, dataframe):
        dimCols=self.getDimCols()
        return dataframe[dataframe.duplicated(subset=dimCols, keep=False)]
