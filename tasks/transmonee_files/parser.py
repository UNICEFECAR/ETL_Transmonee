import re
import pandas as pd

#Pares a transmonee file
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
    #for each row in the csv
    for i in range(len(srcData)):
        #if the row starts with number.number.numner (e.g. 6.1.12) then it is the start of a new indicator
        if re.match("\d+\.\d+.\d+.*", str(srcData.iloc[i][1])):
            indic = srcData.iloc[i][1]
        #if the row (at col number 1)  contains one of the countryes than it is the row containing the values fot that country
        elif str(srcData.iloc[i][1]).strip().lower() in countries:
            country = srcData.iloc[i][1].strip().lower()
            #for all the columns (until a year is found) add the values
            for c in range(3, len(srcData.columns)):
                if not pd.isna(srcData.iloc[yearRow][c]) and re.match("\d{4}", srcData.iloc[yearRow][c]) is not None:
                    parsedData.append({
                        "country": country,
                        "indicator": indic,
                        "year": srcData.iloc[yearRow][c],
                        "value": srcData.iloc[i][c].strip(),
                        "noteId": srcData.iloc[i][2],
                    })
        #if a number is fount ad col 0 then it is a note
        elif re.match("^\d+$", str(srcData.iloc[i][0])):
            parsedNotes.append({
                "indicator": indic,
                "noteId": srcData.iloc[i][0],
                "note": str(srcData.iloc[i][1]).replace("\r\n","").replace("\n","")
            })
        #if 4 digits are in col num 3 and col 1 and 2 are empty then it is a years row: store it for later use
        if re.match("\d{4}", str(srcData.iloc[i][3])) and pd.isna(srcData.iloc[i][2]) and pd.isna(srcData.iloc[i][1]):
            yearRow = i

    #create a pandas dataframe for the data and for the notes
    pd_data = pd.DataFrame.from_dict(parsedData)
    pd_notes = pd.DataFrame.from_dict(parsedNotes)

    #Join the data with the notes
    merged = pd.merge(pd_data, pd_notes, how="left", left_on=['indicator', 'noteId'], right_on=['indicator', 'noteId'])
    merged.drop(columns=['noteId'], inplace=True)
    merged = merged[merged['value'] != "-"]
    merged['unit']=""
    return merged
