import re
import pandas as pd


def parse_transmonee_file(file):
    srcData = pd.read_excel(file, dtype=str)

    indic = ""
    yearRow = -1
    countries = ["albania", "armenia", "azerbaijan", "belarus", "bosnia and herzegovina", "bulgaria", "croatia",
                 "czech republic", "estonia", "georgia", "hungary", "kazakhstan", "kyrgyzstan", "latvia", "lithuania",
                 "moldova", "montenegro", "poland", "romania", "russian federation", "serbia", "slovakia", "slovenia",
                 "tajikistan", "the former yugoslav republic of macedonia", "turkmenistan", "ukraine", "uzbekistan"]

    parsedData = []
    parsedNotes = []
    for i in range(len(srcData)):
        if re.match("\d+\.\d+.\d+.*", str(srcData.iloc[i][1])):
            indic = srcData.iloc[i][1]
        elif str(srcData.iloc[i][1]).strip().lower() in countries:
            country = srcData.iloc[i][1].strip().lower()
            for c in range(3, len(srcData.columns)):
                if re.match("\d{4}", srcData.iloc[yearRow][c]) is not None:
                    parsedData.append({
                        "country": country,
                        "indicator": indic,
                        "year": srcData.iloc[yearRow][c],
                        "value": srcData.iloc[i][c],
                        "noteId": srcData.iloc[i][2],
                    })
        elif re.match("^\d+$", str(srcData.iloc[i][0])):
            parsedNotes.append({
                "indicator": indic,
                "noteId": srcData.iloc[i][0],
                "note": srcData.iloc[i][1]
            })
        if re.match("\d{4}", str(srcData.iloc[i][3])):
            yearRow = i

    pd_data = pd.DataFrame.from_dict(parsedData)
    pd_notes = pd.DataFrame.from_dict(parsedNotes)

    merged = pd.merge(pd_data, pd_notes, how="left", left_on=['indicator', 'noteId'], right_on=['indicator', 'noteId'])
    merged.drop(columns=['noteId'], inplace=True)
    merged = merged[merged['value'] != "-"]
    return merged
