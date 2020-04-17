import fileUtils.FileDownload

import os.path
import pandas as pd
import processData.ColumnMapper as ColumnMapper

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# https://api.uis.unesco.org/sdmx/data/UNESCO,SDG4,2.0/GER.PT.L01.._T..............AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ?startPeriod=2000&endPeriod=2050&format=sdmx-compact-2.0&locale=en&subscription-key=9d48382df9ad408ca538352a4186791b
# https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,3.0/GER.PT....................?startPeriod=2017&endPeriod=2018&format=csv-sdmx&locale=en&subscription-key=3a281766f2124a0fafe10dc27c843931
# https://api.uis.unesco.org/sdmx/data/UNESCO,SDG4,2.0/COMP_EDU.YR.L02................AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ?startPeriod=2000&endPeriod=2050&format=sdmx-compact-2.0&locale=en&subscription-key=9d48382df9ad408ca538352a4186791b

UIS_API_KEY = "9d48382df9ad408ca538352a4186791b"  # Transmonne API Key

UIS_PARAMS = {"url": "https://api.uis.unesco.org/sdmx/data/",
              "headers": {
                  "Accept": "application/vnd.sdmx.data+csv;version=1.0.0",
                  "Accept-Encoding": "gzip",
              }}

DownloadParams_UIS_dfSDG4 = [
    {"dataflowId": ["UNESCO", "SDG4", "2.0"],
     "dq": "GER.PT.L01.._T..............AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "SDG4_01.csv"
     },
    {"dataflowId": ["UNESCO", "SDG4", "2.0"],
     "dq": "FREE_EDU.YR.L02................AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "SDG4_02.csv"
     },
    {"dataflowId": ["UNESCO", "SDG4", "2.0"],
     "dq": "COMP_EDU.YR.L02................AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "SDG4_03.csv"
     }
]

sdg4ToProcess = [
    {
        "in": "SDG4_01.csv",
        "const": {
            # "INDICATOR": "Children in early childhood educational development programs (gross enrolment ratio, % of children aged 0-2)",
            "UNICEF_INDICATOR": "EDU_GER_Y0T2",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "in": "SDG4_02.csv",
        "const": {
            # "INDICATOR": "Number of years of free pre-primary education guaranteed in legal framework (SDG 4.2.5)",
            "UNICEF_INDICATOR": "EDU_FREE_EDU",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "in": "SDG4_03.csv",
        "const": {
            # "INDICATOR": "Number of years of compulsory pre-primary education guaranteed in legal framework (SDG 4.2.5)",
            "UNICEF_INDICATOR": "EDU_COMPEDU",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "", }
    }
]

DownloadParams_UIS_dfEDU_NON_FINANCE = [
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "STU.PER.L02.._T+F+M...INST_T..............AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "EDU_01.csv"
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "STU.PER.L02.._T...INST_PUB..............AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "EDU_02.csv"
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "STU.PER.L02.._T...INST_PRIV..............AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "EDU_03.csv"
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "GECER.PT.L02.._T.................AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "EDU_04.csv"
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "NER.PT.L02.._T.................AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "EDU_05.csv"
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "PRP..L02...................AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "EDU_06.csv"
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "FEP..L02...................AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "EDU_07.csv"
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "NERA...._T+F+M.UNDER1_AGE................AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "EDU_08.csv"
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "NIR.PT.L1.._T+F+M.TH_ENTRY_AGE................AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "out": "EDU_09.csv"
     }
]

eduNonFinToProcess = [
    {
        "in": "EDU_01.csv",
        "const": {
            # "INDICATOR": "Pre-primary (ISCED 02) education enrolments (absolute number)",
            "UNICEF_INDICATOR": "EDU_STU_L02_TOT",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "in": "EDU_02.csv",
        "const": {
            # "INDICATOR": "Pre-primary (ISCED 02) education enrolments in public institutions",
            "UNICEF_INDICATOR": "EDU_STU_L02_PUB",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "in": "EDU_03.csv",
        "const": {
            # "INDICATOR": "Pre-primary (ISCED 02) education enrolments in private institutions",
            "UNICEF_INDICATOR": "EDU_STU_L02_PRIV",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "in": "EDU_04.csv",
        "const": {
            # "INDICATOR": "Enrolment in pre-primary (ISCED 02) education (gross enrolment ratio, % of population aged 3-6)",
            "UNICEF_INDICATOR": "EDU_GECER_L02",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "in": "EDU_05.csv",
        "const": {
            # "INDICATOR": "Enrolment in pre-primary (ISCED 02) education (net enrolment ratio, % of population aged 3-6)",
            "UNICEF_INDICATOR": "EDU_NER_L02",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "in": "EDU_06.csv",
        "const": {
            # "INDICATOR": "Enrolment in private institutions of pre-primary (ISCED 02) education ( % of all children enrolled in respective level of education)",
            "UNICEF_INDICATOR": "EDU_PRP_L02",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "in": "EDU_07.csv",
        "const": {
            # "INDICATOR": "Percentage of females in pre-primary (ISCED 02) education (% of all students enrolled in the respective level of education)",
            "UNICEF_INDICATOR": "EDU_FEP_L02",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "in": "EDU_08.csv",
        "const": {
            # "INDICATOR": "Participation in organized learning (Adjusted net enrolment rate, one year before official primary entry age - Administrative data) (SDG 4.2.2.)",
            "UNICEF_INDICATOR": "EDU_NERA_UNDER1",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        },
        "filterOut": {
            "UNIT_MEASURE": "GPIA"
        }
    },
    {
        "in": "EDU_09.csv",
        "const": {
            # "INDICATOR": "Net Intake Rate to primary education of official entry age",
            "UNICEF_INDICATOR": "EDU_NIR_L1",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },

]

colMap_UNESCO = {
    "Dataflow": {"type": "const", "value": "ECARO:TRANSMONEE(1.0)"},
    "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
    "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
    "SEX": {"type": "col", "role": "dim", "value": "SEX"},
    "AGE": {"type": "col", "role": "dim", "value": "AGE"},
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
    "GE": "GEO",
    "HR": "HRV",
    "KG": "KGZ",
    "KZ": "KAZ",
    "MD": "MDA",
    "ME": "MNE",
    "MK": "MKD",
    "RO": "ROU",
    "RS": "SRB",
    "TJ": "TJK",
    "TM": "TKM",
    "TR": "TUR",
    "UA": "UKR",
    "UZ": "UZB",
}, "UNIT_MEASURE": {
    "PER": "PS",
    "PT": "PCNT",
},
    "AGE":{
        "UNDER1_AGE":"M023",
    }
}


def download_data(outfile_path, skipIfExists=False, verb=0):
    for toDown in DownloadParams_UIS_dfSDG4:
        outFilePath = os.path.join(outfile_path, toDown['out'])
        res = fileUtils.FileDownload.download_sdmx_file(outFilePath, UIS_PARAMS['url'], toDown['dataflowId'],
                                                        toDown["dq"],
                                                        params=toDown["params"], headers=UIS_PARAMS['headers'],
                                                        apikey=UIS_API_KEY, skipIfExists=skipIfExists)
        if verb >= 3:
            print(toDown['out'] + " " + res)

    for toDown in DownloadParams_UIS_dfEDU_NON_FINANCE:
        outFilePath = os.path.join(outfile_path, toDown['out'])
        fileUtils.FileDownload.download_sdmx_file(outFilePath, UIS_PARAMS['url'], toDown['dataflowId'], toDown["dq"],
                                                  params=toDown["params"], headers=UIS_PARAMS['headers'],
                                                  apikey=UIS_API_KEY, skipIfExists=skipIfExists)
        if verb >= 3:
            print(toDown['out'] + " " + res)


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
    for p in sdg4ToProcess:
        toAdd = _process(os.path.join(workingPath, p['in']), p, codemap_UNESCO, colMap_UNESCO, p['const'])
        ret = ret.append(toAdd)
    for p in eduNonFinToProcess:
        toAdd = _process(os.path.join(workingPath, p['in']), p, codemap_UNESCO, colMap_UNESCO, p['const'])
        ret = ret.append(toAdd)
    return ret
