import fileUtils.FileDownload

import os.path
import pandas as pd
import processData.ColumnMapper as ColumnMapper

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# https://api.uis.unesco.org/sdmx/data/UNESCO,SDG4,2.0/GER.PT.L01.._T..............AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ?startPeriod=2000&endPeriod=2050&format=sdmx-compact-2.0&locale=en&subscription-key=9d48382df9ad408ca538352a4186791b

UIS_API_KEY = "9d48382df9ad408ca538352a4186791b"  # Transmonne API Key

UIS_PARAMS = {"url": "https://api.uis.unesco.org/sdmx/data/",
              "headers": {
                  "Accept": "application/vnd.sdmx.data+csv;version=1.0.0",
                  "Accept-Encoding": "gzip",
              }}

_toProcess_UIS_dfSDG4 = [
    {"dataflowId": ["UNESCO", "SDG4", "2.0"],
     "dq": "GER.PT.L01.._T..............AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "SDG4_01.csv",
     "const": {
         # "INDICATOR": "Children in early childhood educational development programs (gross enrolment ratio, % of children aged 0-2)",
         "UNICEF_INDICATOR": "EDU_GER_Y0T2",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": ""}
     },
    {"dataflowId": ["UNESCO", "SDG4", "2.0"],
     "dq": "FREE_EDU.YR.L02................AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "SDG4_02.csv",
     "const": {
         # "INDICATOR": "Number of years of free pre-primary education guaranteed in legal framework (SDG 4.2.5)",
         "UNICEF_INDICATOR": "EDU_FREE_EDU",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": ""}

     },
    {"dataflowId": ["UNESCO", "SDG4", "2.0"],
     "dq": "COMP_EDU.YR.L02................AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "SDG4_03.csv",
     "const": {
         # "INDICATOR": "Number of years of compulsory pre-primary education guaranteed in legal framework (SDG 4.2.5)",
         "UNICEF_INDICATOR": "EDU_COMPEDU",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": "", }
     }
]

_toProcess_UIS_dfEDU_NON_FINANCE = [
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "STU.PER.L02.._T+F+M...INST_T..............AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "EDU_01.csv",
     "const": {
         # "INDICATOR": "Pre-primary (ISCED 02) education enrolments (absolute number)",
         "UNICEF_INDICATOR": "EDU_STU_L02_TOT",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": "",
     }
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "STU.PER.L02.._T...INST_PUB..............AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "EDU_02.csv",
     "const": {
         # "INDICATOR": "Pre-primary (ISCED 02) education enrolments in public institutions",
         "UNICEF_INDICATOR": "EDU_STU_L02_PUB",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": "",
     }
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "STU.PER.L02.._T...INST_PRIV..............AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "EDU_03.csv",
     "const": {
         # "INDICATOR": "Pre-primary (ISCED 02) education enrolments in private institutions",
         "UNICEF_INDICATOR": "EDU_STU_L02_PRIV",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": "",
     }
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "GECER.PT.L02.._T.................AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "EDU_04.csv",
     "const": {
         # "INDICATOR": "Enrolment in pre-primary (ISCED 02) education (gross enrolment ratio, % of population aged 3-6)",
         "UNICEF_INDICATOR": "EDU_GECER_L02",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": "",
     }
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "NER.PT.L02.._T.................AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "EDU_05.csv",
     "const": {
         # "INDICATOR": "Enrolment in pre-primary (ISCED 02) education (net enrolment ratio, % of population aged 3-6)",
         "UNICEF_INDICATOR": "EDU_NER_L02",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": "",
     }
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "PRP..L02...................AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "EDU_06.csv",
     "const": {
         # "INDICATOR": "Enrolment in private institutions of pre-primary (ISCED 02) education ( % of all children enrolled in respective level of education)",
         "UNICEF_INDICATOR": "EDU_PRP_L02",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": "",
     }
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "FEP..L02...................AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "EDU_07.csv",
     "const": {
         # "INDICATOR": "Percentage of females in pre-primary (ISCED 02) education (% of all students enrolled in the respective level of education)",
         "UNICEF_INDICATOR": "EDU_FEP_L02",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": "",
     }
     },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "NERA...._T+F+M.UNDER1_AGE................AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "EDU_08.csv",
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
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "NIR.PT.L1.._T+F+M.TH_ENTRY_AGE................AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "EDU_09.csv",
     "const": {
         # "INDICATOR": "Net Intake Rate to primary education of official entry age",
         "UNICEF_INDICATOR": "EDU_NIR_L1",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": "",
     }
     },
    {
        # Will generate duplicates if we get the Disaggregations by grade and type (Adult, initial).
        "dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
        "dq": "STU.PER.L1.._T+F+M...INST_T..............AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
        "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
        "tmp_file": "EDU_10.csv",
        "const": {
            # "INDICATOR": "Primary education (ISCED 1) enrolments (absolute number)",
            "UNICEF_INDICATOR": "EDU_STU_L1",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {"dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
     "dq": "FEP..L1...................AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
     "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
     "tmp_file": "EDU_11.csv",
     "const": {
         # "INDICATOR": "Percentage of females in primary  (ISCED 1) education (% of all students enrolled in the respective level of education)",
         "UNICEF_INDICATOR": "EDU_FEP_L1",
         "Dataflow": "ECARO:TRANSMONEE(1.0)",
         "DATA_SOURCE": "UIS",
         "OBS_FOOTNOTE": "",
     }
     },
    {
        # Do we need all the disaggr? Grade and Orientation (GENERAL, VOCATIONAL)
        "dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
        "dq": "STU.PER.L2.._T+F+M...INST_T..............AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
        "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
        "tmp_file": "EDU_12.csv",
        "const": {
            # "INDICATOR": "Lower Secondary education (ISCED 2) enrolments (absolute number)",
            "UNICEF_INDICATOR": "EDU_STU_L2",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
        "dq": "FEP..L2...................AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
        "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
        "tmp_file": "EDU_13.csv",
        "const": {
            # "INDICATOR": "Percentage of females in lower secondary  (ISCED 2) education (% of all students enrolled in the respective level of education)",
            "UNICEF_INDICATOR": "EDU_FEP_L2",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    {
        "dataflowId": ["UNESCO", "EDU_NON_FINANCE", "3.0"],
        "dq": "STU.PER.L3._T._T+F+M...INST_T..............AL+AM+AZ+BA+BG+BY+CZ+EE+GE+HR+HU+KG+KZ+LT+LV+MD+ME+MK+PL+RO+RS+RU+SI+SV+TJ+TM+TR+UA+UZ",
        "params": {"startPeriod": "2000", "endPeriod": "2050", "locale": "en"},
        "tmp_file": "EDU_14.csv",
        "const": {
            # "INDICATOR": "Upper secondary education (ISCED 3) enrolment (absolute number)",
            "UNICEF_INDICATOR": "EDU_STU_L3",
            "Dataflow": "ECARO:TRANSMONEE(1.0)",
            "DATA_SOURCE": "UIS",
            "OBS_FOOTNOTE": "",
        }
    },
    # UIS https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,3.0/STU.PER.L3._T._T+F+M...INST_T..............AL+AM+AZ+BA+BG+BY+GE+HR+KG+KZ+MD+ME+MK+RO+RS+TJ+TM+TR+UA+UZ?startPeriod=2000&endPeriod=2050&format=sdmx-compact-2.0&locale=en&subscription-key=9d48382df9ad408ca538352a4186791b

]

colMap_UNESCO = {
    "Dataflow": {"type": "const", "value": "ECARO:TRANSMONEE(1.0)"},
    "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
    "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
    "SEX": {"type": "col", "role": "dim", "value": "SEX"},
    "AGE": {"type": "col", "role": "dim", "value": "AGE"},
    "GRADE": {"type": "col", "role": "dim", "value": "GRADE"},
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
    }
}


def download_data(outfile_path, skipIfExists=False, verb=0):
    for toDown in _toProcess_UIS_dfSDG4:
        outFilePath = os.path.join(outfile_path, toDown['tmp_file'])
        res = fileUtils.FileDownload.download_sdmx_file(outFilePath, UIS_PARAMS['url'], toDown['dataflowId'],
                                                        toDown["dq"],
                                                        params=toDown["params"], headers=UIS_PARAMS['headers'],
                                                        apikey=UIS_API_KEY, skipIfExists=skipIfExists)
        if verb >= 3:
            print(toDown['tmp_file'] + " " + res)

    for toDown in _toProcess_UIS_dfEDU_NON_FINANCE:
        outFilePath = os.path.join(outfile_path, toDown['tmp_file'])
        res = fileUtils.FileDownload.download_sdmx_file(outFilePath, UIS_PARAMS['url'], toDown['dataflowId'],
                                                        toDown["dq"],
                                                        params=toDown["params"], headers=UIS_PARAMS['headers'],
                                                        apikey=UIS_API_KEY, skipIfExists=skipIfExists)
        if verb >= 3:
            print(toDown['tmp_file'] + " " + res)


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
    for p in _toProcess_UIS_dfSDG4:
        toAdd = _process(os.path.join(workingPath, p['tmp_file']), p, codemap_UNESCO, colMap_UNESCO, p['const'])
        ret = ret.append(toAdd)
    for p in _toProcess_UIS_dfEDU_NON_FINANCE:
        toAdd = _process(os.path.join(workingPath, p['tmp_file']), p, codemap_UNESCO, colMap_UNESCO, p['const'])
        ret = ret.append(toAdd)
    return ret
