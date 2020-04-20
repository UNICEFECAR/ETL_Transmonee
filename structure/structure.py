class dsd:
    '''
    Holds a data structure, typically to conform to a DSD
    '''

    def __init__(self, dsd):
        self.dsd = dsd

    # def get_dims(self):
    #     '''
    #     returns the Dimensions,
    #     :return:
    #     '''
    #     cols = []
    #     for c in colMapping:
    #         if "role" in colMapping[c] and colMapping[c]['type'] == "col":
    #             if colMapping[c]['role'] == "dim" or colMapping[c]['role'] == "time":
    #                 cols.append(colMapping[c]['value'])
    #     return cols

    def getCSVColumns(self):
        return [c['id'] for c in self.dsd]
