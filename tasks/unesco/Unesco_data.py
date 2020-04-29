import os
import pandas as pd
import processData.ColumnMapper as ColumnMapper
import processData.CodeMapper as CodeMapper
import fileUtils.CsvUtils
import fileUtils.FileDownload as file_down
import tasks.unesco.taskcfg as cfg


def _download_data(to_down, outfile_path, skipIfExists=False, verb=0):
    outFilePath = os.path.join(outfile_path, to_down['tmp_file'])
    res = file_down.download_sdmx_file(outFilePath, cfg.URL_PARAMS['url'], to_down['dataflowId'],
                                       to_down["dq"],
                                       params=to_down["params"],
                                       headers=cfg.URL_PARAMS['headers'],
                                       apikey=cfg.API_KEY, skipIfExists=skipIfExists)
    if verb >= 3:
        print(to_down['tmp_file'] + " " + res)


def getData(source_config_file, temp_dir, codemap, colmap, destination_cols, filterFun=None, skipIfExists=False,
            verb=0):
    datalist = fileUtils.CsvUtils.readDictionaryFromCSV(source_config_file)
    toProcess = []
    for l in datalist:
        toadd = {"dataflowId": [l['agency'], l['df'], l['ver']],
                 "dq": l["dq"],
                 "params": cfg.QUERY_PARAMS,
                 "tmp_file": l["tmp_file"],
                 "const": {
                 }
                 }
        for k in l:
            if k.startswith("c_"):
                toadd["const"][k.replace("c_", "")] = l[k]
        toProcess.append(toadd)

    for p in toProcess:
        _download_data(p, temp_dir, skipIfExists=skipIfExists, verb=verb)

    colmapper = ColumnMapper.ColumnMapper(colmap)
    ret = pd.DataFrame(columns=destination_cols, dtype=str)

    for p in toProcess:
        data = pd.read_csv(os.path.join(temp_dir, p["tmp_file"]), dtype=str)

        if filterFun is not None:
            data = filterFun(data)

        duplicateWarning = colmapper.getDuplicates(data)
        if not duplicateWarning.empty:
            print(p['tmp_file'] + " will generate duplicates")
        if codemap is not None:
            data = CodeMapper.map_codes(data, codemap)

        data = colmapper.mapDataframe(data, p["const"])

        ret = ret.append(data)

    return ret
