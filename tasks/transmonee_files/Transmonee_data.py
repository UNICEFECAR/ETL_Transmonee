import tasks.transmonee_files.parser as parser
import processData.ColumnMapper as ColumnMapper
import processData.CodeMapper as CodeMapper


def getData(source_file, codemap, colmap, consts):
    data = parser.parse_transmonee_file(source_file)
    # add the sex col
    data["sex"] = "_T"
    data["age"] = "_T"
    data = CodeMapper.map_codes(data, codemap)
    data = ColumnMapper.ColumnMapper(colmap).mapDataframe(data, consts)
    return data
