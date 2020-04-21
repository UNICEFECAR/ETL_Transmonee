import requests
import os
#Utils to download files.


def _download_data(addr, params=None, headers=None):
    try:
        response = requests.get(addr, params=params, headers=headers)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
        raise
    except Exception as err:
        print(f'Other error occurred: {err}')
        raise
    return response.content


def download_file(destinationFile, addr, params=None, headers=None, skipIfExists=False):
    '''Downloads data and saves to file

    :param destinationFile: The output file
    :param addr: the address to download data from
    :param params: the GET parameters
    :param headers: the GET headers
    :param skipIfExists: skip if file is already present
    :return: str:"skipped" if skipped, "ok" if downloaded
    '''

    if skipIfExists and os.path.exists(destinationFile):
        return "skipped"

    data = _download_data(addr, params, headers)
    with open(destinationFile, 'wb') as f:
        f.write(data)
    return "downloaded"


def download_sdmx_file(destinationFile, addr, dataflowId, dq, params, headers=None, apikey=None, skipIfExists=False):
    '''
    Downloads a SDMX file, it is just a wrapper to build the call in SDMX format
    :param destinationFile: The output file
    :param addr: the address to download data from
    :param dataflowId: The id of the SMDX dataflow
    :param dq: The SDMX data query
    :param params: the GET params
    :param headers:the GET headers
    :param apikey: optional API key
    :param skipIfExists:  skip if file is already present
    :return: str:"skipped" if skipped, "downloaded" if downloaded
    '''
    toCall = (addr + ",".join(dataflowId) + "/" + dq)
    if (apikey is not None):
        params["subscription-key"] = apikey
    ret = download_file(destinationFile, toCall, params, headers, skipIfExists)
    return ret


def download_eurostat_sdmx_file(destinationFile, addr, dataflowId, dq, params, headers=None, skipIfExists=False):
    '''
    Downloads a SDMX file from EUROSTAT , it is just a wrapper to build the call in EUROSTAT-SDMX format
    :param destinationFile: The output file
    :param addr: the address to download data from
    :param dataflowId: The id of the SMDX dataflow
    :param dq: The SDMX data query
    :param params: the GET params
    :param headers:the GET headers
    :param apikey: optional API key
    :param skipIfExists:  skip if file is already present
    :return: str:"skipped" if skipped, "ok" if downloaded
    '''
    toCall = addr + "/" + dataflowId + "/" + dq
    return download_file(destinationFile, toCall, params, headers=headers, skipIfExists=skipIfExists)
