API_KEY = "9d48382df9ad408ca538352a4186791b"  # Transmonne API Key

URL_PARAMS = {"url": "https://api.uis.unesco.org/sdmx/data/",
              "headers": {
                  "Accept": "application/vnd.sdmx.data+csv;version=1.0.0",
                  "Accept-Encoding": "gzip",
              }}

QUERY_PARAMS = {"startPeriod": "1989", "endPeriod": "2050", "locale": "en"}

SOURCE_CONFIG_SDG4 = "source_configs\\unesco\\EDU_UIS_SDG4_toDown.csv"
SOURCE_CONFIG_EDUNONFIN = "source_configs\\unesco\\EDU_UIS_EDUNONFIN_toDown.csv"
SOURCE_CONFIG_EDUFIN = "source_configs\\unesco\\EDU_UIS_EDUFIN_toDown.csv"

# The mapping between the data and the DSD columns
colmap_SDG4_EDUNONFIN = {
    "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
    "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
    "SEX": {"type": "col", "role": "dim", "value": "SEX"},
    "AGE": {"type": "col", "role": "dim", "value": "AGE"},
    "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
    "RESIDENCE": {"type": "col", "role": "dim", "value": "LOCATION"},
    "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
    "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
    "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
    "OBS_FOOTNOTE": {"type": "const", "role": "attrib", },
    "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
    "DATA_SOURCE": {"type": "const", "role": "attrib", "value": "UIS"},
    "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
    "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
}

# the country mapping for unesco
country_map = {"REF_AREA": {
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
}}
# Additional mappings for SDG4 and EDUNONFIN
codemap_SDG4_EDUNONFIN = {
    "UNIT_MEASURE": {
        "PER": "PS",
        "PT": "PCNT",
        "NB": "NUMBER",
    },
    "AGE": {
        "UNDER1_AGE": "M023",
    },
    "LOCATION": {
        "RUR": "R",
        "URB": "U",
        "_Z": "_T",
        "_T": "_T"
    },
    "WEALTH_QUINTILE": {
        "_Z": "_T"
    }
}

# UNESCO EDUFinance dataflow
codemap_EDUFIN = {
    "UNIT_MEASURE": {"GDP": "GDP_PERC"}
}
colmap_EDUFIN = {
    "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
    "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
    "SEX": {"type": "const", "role": "dim"},
    "AGE": {"type": "const", "role": "dim"},
    "WEALTH_QUINTILE": {"type": "const", "role": "dim"},
    "RESIDENCE": {"type": "const", "role": "dim"},
    "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
    "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
    "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
    "OBS_FOOTNOTE": {"type": "const", "role": "attrib", },
    "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
    "DATA_SOURCE": {"type": "const", "role": "attrib", "value": "UIS"},
    "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
    "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
}
