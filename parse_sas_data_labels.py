import re
import pandas as pd

def read_data(path):
    return open(path).read()

def extract_data_by_key(data, key):
    return re.findall(f'(?s)(?<={key})(.*?)(?=;)', data)[0]

def extract_states(data, key, output_path):
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

output_path = 'output_data'
extract_countries(contents, 'i94cntyl', output_path)
extract_ports(contents, 'i94prtl', output_path)
extract_states(contents, 'i94addrl', output_path)