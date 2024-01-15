#!/usr/bin/python
from configparser import ConfigParser

def config(filename='config/database.ini', section='postgresql'):
# create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
 
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
            if param[0] == 'port':
                db[param[0]] = int(param[1])
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db

def stepconfig():
    filename='config/database.ini'
    section='step'
    parser = ConfigParser()
    parser.read(filename)
    if parser.has_section(section):
        ret = parser.get(section, 'curstep')
        return int(ret)
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    
def stepsetconfig(curstep=0):
    filename='config/database.ini'
    section='step'
    parser = ConfigParser()
    parser.read(filename)
    if parser.has_section(section):
        parser[section]['curstep'] = str(curstep)
        with open(filename, 'w') as configfile:    # save
            parser.write(configfile)
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
