import pandasdmx as sdmx
import pandasdmx.api
import pandas as pd
import sys
import os


# http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/spr_exp_gdp/latest
# http://ec.europa.eu/eurostat/SDMX/diss-web/rest/datastructure/ESTAT/DSD_spr_exp_gdp

def sdmx_xml2pandas(file_path):
    '''
    Converts an SDMX-XML file in a PAndas DataFrame
    :param file_path: the SDMX file to load
    :return: a pandas dataframe
    '''
    data = sdmx.read_sdmx(file_path)
    #The library contains some prints, removed redirecting the stdout
    original = sys.stdout
    sys.stdout = open(os.devnull, "w")
    vals = sdmx.writer.write_dataset(data.data[0], attributes='o')
    sys.stdout = original

    # If it is a serie convert to dataframe
    if (isinstance(vals, pd.Series)):
        vals = vals.to_frame()
    # remove the index and return columns
    vals.reset_index(inplace=True)
    vals.fillna('', inplace=True)
    return vals
