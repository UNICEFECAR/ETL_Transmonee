class dsd:
    '''
    Holds a data structure, typically to conform to a DSD
    '''

    def __init__(self, dsd):
        self.dsd = dsd

    def get_dims(self):
        '''
        returns the Dimensions,
        :return:
        '''
        ret = []
        for c in self.dsd:
            if "role" in c and (c['role'] == "dim" or c['role'] == "time"):
                ret.append(c['id'])
        return ret

    def getCSVColumns(self):
        return [c['id'] for c in self.dsd]
