#maps codes between two coding systems
def map_codes(data, codemap):
    DEP = "depends"
    MAP = "map"
    if codemap is None:
        return data

    for col in codemap:
        #handles maps dependent on another column (e.g. if indicator is Population then Unit of measure is Number)
        if DEP in codemap[col]:
            #for each code
            for m in codemap[col][MAP]:
                #create a mask: if source column is == m than change the code
                sourceCol = codemap[col][DEP]
                mask = data[sourceCol] == m
                data[col][mask] = codemap[col][MAP][m]
        else:
            #Simpler case: if code in source dataset is , then replace with the map
            for m in codemap[col]:
                data[col].replace(m, codemap[col][m], inplace=True)

    return data
