def load(filename):
    fileHandle = open(filename)
    abrevToState = dict()
    for line in fileHandle.readlines():
        parts = line.strip().split(' - ')
        abrevToState[parts[1].lower()] = parts[0]
        #print(parts[0])
        #print(parts[1])
    return abrevToState

def parse_place(placestr, abrevmap):
    abrev = placestr.split(',')[-1]
    if abrev.lower() not in abrevmap:
        return (False, "None")
    return abrevmap[abrev]