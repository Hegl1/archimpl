def load_file(path):
    """Loads a table from a file.
    This function extracts a table from a specific file format.
    The function returns a tuple that contains 2 tuple lists.
    The first tuple list represents the schema of the table:
    [(column_name1, datatype1),(column_name2, datatype2) ... ]
    The second tuple list represents the datarows of the table:
    [(row1_field1,row1_field2,...),(row2_field1,row2_field2,...)...]
    """
    schema = []
    data = []
    with open(path,"r") as f:
        lines = f.readlines()
        i = 0

        #get first and last line of schema
        while lines[i] != "[Schema]\n":
            i+= 1

        schema_end = i

        while lines[schema_end] != "\n":
            schema_end+= 1

        #retrieve schema and convert it into tuple list
        for j in range(i+1,schema_end):
            schema.append(tuple(lines[j].rstrip('\n').replace(" ","").split(':')))

        i = schema_end

        #get first line of data
        while lines[i] != "[Data]\n":
            i+= 1

        #retrieve data and convert into tuple list
        for j in range(i+1,len(lines)):
            data.append(tuple(lines[j].rstrip('\n').split(';')))

    return (schema,data)