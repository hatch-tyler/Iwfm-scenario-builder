def read_data(file_object):
    while True:
        # read next line in file
        data = file_object.readline()

        # exit if no data read
        if len(data) == 0:
            break

        # get the first non-whitespace character from data
        first_char = data.lstrip()[0]

        # check for comment characters
        if first_char not in ["C", "c", "*"]:
            break

    return read_until_substring(data, ' /')


def read_until_character(string, character):
    for i, ch in enumerate(string):
        if ch == character:
            return string[:i].strip()

    return string.strip()


def read_until_substring(string, substring):
    # convert tabs to spaces in string for simplicity
    string = string.replace('\t', ' ')
    
    # find index of substring in string
    idx = string.find(substring)

    if idx:
        return string[:idx].strip()

    return string.strip()