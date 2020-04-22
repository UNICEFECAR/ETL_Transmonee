import csv

def readDictionaryFromCSV(file, encoding="utf-8"):
    ret = []
    with open(file, encoding=encoding) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for row in reader:
            ret.append(row)
    return ret