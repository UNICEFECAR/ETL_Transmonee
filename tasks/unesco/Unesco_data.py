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
            print(p + " will generate duplicates")
        if codemap is not None:
            data = CodeMapper.map_codes(data, codemap)

        data = colmapper.mapDataframe(data, p["const"])

        ret = ret.append(data)

    return ret


'''


    def _process(self, input_file, col_map, constants, codeMap=None, filterFun=None, check_for_dups=True):
        src = pd.read_csv(input_file, dtype=str)
        colMapper = ColumnMapper.ColumnMapper(col_map)

        if filterFun is not None:
            src = filterFun(src)

        if check_for_dups:
            duplicateWarning = colMapper.getDuplicates(src)
            if not duplicateWarning.empty:
                print(input_file + " will generate duplicates")

        if codeMap is not None:
            for col in codeMap:
                for m in codeMap[col]:
                    src[col].replace(m, codeMap[col][m], inplace=True)

        return colMapper.mapDataframe(src, constants)

    def getdata(self, workingPath, cols, columnMap, filterFunction=None, codeMap=None):
        ret = pd.DataFrame(columns=cols, dtype=str)
        for p in self._toProcess:
            toAdd = self._process(os.path.join(workingPath, p['tmp_file']), columnMap, p['const'], codeMap=codeMap,
                                  filterFun=filterFunction)
            ret = ret.append(toAdd)
        return ret

'''
