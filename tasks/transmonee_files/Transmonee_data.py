import tasks.transmonee_files.parser as parser
import processData.ColumnMapper as ColumnMapper
import pandas as pd


class TransmoneeData:
    colMap = {
        "REF_AREA": {"type": "col", "role": "dim", "value": "country"},
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "indicator"},
        "SEX": {"type": "const", "role": "dim"},
        "AGE": {"type": "const", "role": "dim"},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim"},
        "RESIDENCE": {"type": "const", "role": "dim"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "year"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "value"},
        "UNIT_MEASURE": {"type": "const", "role": "attrib"},
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", },
        "FREQ": {"type": "const", "role": "attrib"},
        "DATA_SOURCE": {"type": "const", "role": "attrib"},
        "UNIT_MULTIPLIER": {"type": "const", "role": "attrib"},
        "OBS_STATUS": {"type": "const", "role": "attrib"},
    }

    const = {
        "SEX": "_T",
        "AGE": "_T",
        "WEALTH_QUINTILE": "_T",
        "RESIDENCE": "_T",
        "UNIT_MEASURE": "PCNT",
        "OBS_FOOTNOTE": "",
        "FREQ": "",
        "DATA_SOURCE": "",
        "UNIT_MULTIPLIER": "",
        "OBS_STATUS": ""
    }

    codeMap = {"country": {
        "albania": "ALB",
        "armenia": "ARM",
        "azerbaijan": "AZE",
        "belarus": "BLR",
        "bosnia and herzegovina": "BIH",
        "bulgaria": "BGR",
        "croatia": "HRV",
        "czech republic": "CZE",
        "estonia": "EST",
        "georgia": "GEO",
        "hungary": "HUN",
        "kazakhstan": "KAZ",
        "kyrgyzstan": "KGZ",
        "latvia": "LVA",
        "lithuania": "LTU",
        "moldova": "MDA",
        "montenegro": "MNE",
        "poland": "POL",
        "romania": "ROU",
        "russian federation": "RUS",
        "serbia": "SRB",
        "slovakia": "SVK",
        "slovenia": "SVN",
        "tajikistan": "TJK",
        "the former yugoslav republic of macedonia": "MKD",
        "turkmenistan": "TKM",
        "ukraine": "UKR",
        "uzbekistan": "UZB",
    },
        "indicator": {
            "8.1.1 Total Social Protection expenditure as % of GDP": "SP_TOT",
            "8.1.2 Expenditure on cash social benefits as % of GDP": "SP_SOC_BEN_CASH",
            "8.1.3 Expenditure on social benefits in kind as % of GDP": "SP_BEN_KIND",
            "8.1.4 Expenditure on social benefits under Family/Children function as % of GDP": "SP_BEN_FAMILY",
            "8.1.5 Expenditure on cash social benefits under Family/Children function as % of GDP": "SP_BEN_FAMILY_CASH",
            "8.1.6 Expenditure on social benefits in kind under Family/Children function as % of GDP": "SP_BEN_FAMILY_KIND",
            "8.1.7 Expenditure on means-tested social protection benefits as % of total social protection expenditure ": "SP_MEANS_TESTED",
            "8.1.8 Expenditure on cash social benefits as % of total social protection expenditure": "SP_CASH_EXP",
            "8.1.9 Expenditure on social benefits in kind as % of total social protection expenditure": "SP_KIND_EXP",
            "8.1.10 Expenditure on social benefits under Family/Children function as % of total social protection expenditure ": "SP_BEN_FAMILY_EXP",
            "8.1.11 Social benefits in kind under family/children function as % of total social benefits in kind  ": "SP_KIND_FAMILY_KIND",
            "8.1.12 Expenditure on Family allowances as % of total cash expenditure under family/children function ": "SP_FAMILY_ALLOW",
            "8.1.13 Expenditure on Income compensation during maternity as % of total cash expenditure under family/children function ": "SP_IMCOME_MATERNITY",
            "8.1.14  Expenditure on Parental leave as % of total cash expenditure under family/children function ": "SP_PARENTAL_LEAVE",
            "8.1.15 Expenditure on Birth grant as % of total cash expenditure under family/children function ": "SP_BITRH_GRANT",
            "8.1.16 Expenditure on other family cash social benefits as % of total cash expenditure under family/children function ": "SP_OTHER_FAMILY",

        }

    }

    def __init__(self, source_file):
        self._source_file = source_file

    def _process(self, data, col_map, constants, codeMap=None):
        colMapper = ColumnMapper.ColumnMapper(col_map)

        if codeMap is not None:
            for col in codeMap:
                for m in codeMap[col]:
                    data[col].replace(m, codeMap[col][m], inplace=True)

        mapped = colMapper.mapDataframe(data, constants)
        # print(mapped)
        return mapped

    def getdata(self, cols, columnMap, filterFunction=None, codeMap=None):
        ret = pd.DataFrame(columns=cols, dtype=str)
        # ret=[]
        srcdata = parser.parse_transmonee_file(self._source_file)
        srcdata = self._process(srcdata, TransmoneeData.colMap, TransmoneeData.const, TransmoneeData.codeMap)
        ret = ret.append(srcdata)
        return ret
        # return ret

    '''
            ret = pd.DataFrame(columns=cols, dtype=str)
        for p in self._toProcess:
            toAdd = self._process(os.path.join(workingPath, p['tmp_file']), columnMap, p['const'], codeMap=codeMap,
                                  filterFun=filterFunction)
            # self.append = ret.append(toAdd)
            # ret = self.append
            ret.append(toAdd)
            
            '''
