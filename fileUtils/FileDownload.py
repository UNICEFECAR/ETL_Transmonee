import requests
import os


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
    if skipIfExists and os.path.exists(destinationFile):
        return "skipped"

    data = _download_data(addr, params, headers)
    with open(destinationFile, 'wb') as f:
        f.write(data)
    return "ok"


def download_sdmx_file(destinationFile, addr, dataflowId, dq, params, headers=None, apikey=None, skipIfExists=False):
    toCall = (addr + ",".join(dataflowId) + "/" + dq)
    if (apikey is not None):
        params["subscription-key"] = apikey
    return download_file(destinationFile, toCall, params, headers, skipIfExists)


def download_eurostat_sdmx_file(destinationFile, addr, dataflowId, dq, params, headers=None, skipIfExists=False):
    toCall = addr + "/" + dataflowId + "/" + dq
    return download_file(destinationFile, toCall, params, headers=headers, skipIfExists=skipIfExists)
