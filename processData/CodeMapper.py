def map_codes(data, codemap):
    DEP = "depends"
    MAP = "map"
    if codemap is None:
        return data

    for col in codemap:
        if DEP in codemap[col]:
            for m in codemap[col][MAP]:
                sourceCol = codemap[col][DEP]
                mask = data[sourceCol] == m
                data[col][mask] = codemap[col][MAP][m]
        else:
            for m in codemap[col]:
                data[col].replace(m, codemap[col][m], inplace=True)

    return data