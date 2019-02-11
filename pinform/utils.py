

def dromedary_to_underline(s: str) -> str:
    if s[0].islower():
        return ''.join((x if x.isalnum() and x.islower() else '_' + x.lower()) for x in s)
    else:
        return ''.join((x if x.isalnum() and x.islower() else '_' + x.lower()) for x in s)[1:]


def underline_to_dromedary(s: str) -> str:
    sp = ''
    underline = False
    for x in s:
        if x == '_':
            underline = True
        if x.isalnum():
            if underline:
                underline = False
                sp += x.upper()
            else:
                sp += x.lower()
    return sp
