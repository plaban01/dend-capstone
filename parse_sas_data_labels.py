import re
import pandas as pd

def read_data(path):
    ''' This method reads the contents of a file

    INPUTS:
        path: Path of the file

    '''
    return open(path).read()

def extract_data_by_key(data, key):
    ''' This method extracts data from data dictionary using a given pattern

    INPUTS:
        data: File contents
        key: Pattern to be used for extraction
    '''
    return re.findall(f'(?s)(?<={key})(.*?)(?=;)', data)[0]

def extract_states(data, key, output_path):
    ''' This method extracts the states from the SAS data definitions

    INPUTS:
        data: SAS data definitions
        key: pattern for identifying the section containing state information
        output_path: Path of the output file
    '''
    states = list(
        map(
            lambda x: (x[0].strip().replace("'", ""), x[1].strip().replace("'", "")),
            map(
                lambda x: x.split('='), 
                extract_data_by_key(data, key).split("\n")[1:]
            )
        )
    )

    # Write states csv
    df = pd.DataFrame(states, columns=['state_code', 'state_name'])
    df.to_csv(f'{output_path}/i94_states.csv', index=False)

def extract_ports(data, key, output_path):
    ''' This method extracts the ports from the SAS data definitions

    INPUTS:
        data: SAS data definitions
        key: pattern for identifying the section containing ports information
        output_path: Path of the output file
    '''
    ports = list(
        map(
            lambda x: (
                x[0].strip().replace("'", ""),
                x[1].split(',')[0].strip().replace("'", "").strip(),
                x[1].split(',')[1].strip().replace("'", "").split(' ')[0].strip() if  len(x[1].split(',')) > 1 else ''
            ),
            map(
                lambda x: x.split('='), 
                extract_data_by_key(data, key).split("\n")[1:-1]
            )
        )
    )

    # Write ports csv
    df = pd.DataFrame(ports, columns=['port_code', 'port_name', 'state_code'])
    df.to_csv(f'{output_path}/i94_ports.csv', index=False)

def extract_countries(data, key, output_path):
    ''' This method extracts the countries from the SAS data definitions

    INPUTS:
        data: SAS data definitions
        key: pattern for identifying the section containing state information
        output_path: Path of the output file
    '''
    countries = list(
        map(
            lambda x: (x[0].strip(), x[1].strip().replace("'", "")), 
            map(
                lambda x: x.split('='), 
                extract_data_by_key(data, key).split("\n")[1:]
            )
        )
    )

    # Cleanup extra text after MEXICO
    countries[0] = (countries[0][0], countries[0][1].split(' ')[0])
    
    # Write countries csv
    df = pd.DataFrame(countries, columns=['country_code', 'country_name'])
    df.to_csv(f'{output_path}/i94_countries.csv', index=False)

contents = read_data('data/I94_SAS_Labels_Descriptions.SAS')

output_path = '.'
extract_countries(contents, 'i94cntyl', output_path)
extract_ports(contents, 'i94prtl', output_path)
extract_states(contents, 'i94addrl', output_path)