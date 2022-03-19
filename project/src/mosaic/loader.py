def load_data(path):
    table = []
    with open(path,"r") as f:
        lines = f.readlines()
        i = 0
        while lines[i] != "[Data]\n":
            i+= 1

        for j in range(i+1,len(lines)):
            table.append(tuple(lines[j].rstrip('\n').split(';')))

    return table