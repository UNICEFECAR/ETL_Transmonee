import tasks.transmonee_files.parser as parser
import processData.ColumnMapper as ColumnMapper
import processData.CodeMapper as CodeMapper

#groups together the actions needed to return data in a common format
def getData(source_file, codemap, colmap, consts):
    #parses tge transmonee data
    data = parser.parse_transmonee_file(source_file)
    # add the sex and age col
    data["sex"] = "_T"
    data["age"] = "_T"
    #maps the codes
    data = CodeMapper.map_codes(data, codemap)
    #maps the cols
    data = ColumnMapper.ColumnMapper(colmap).mapDataframe(data, consts)
    return data
