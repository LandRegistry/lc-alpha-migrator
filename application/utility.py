import re
import os
import json

def convert_class(class_of_charge):
    charge = {
        "C1": "C(I)",
        "C2": "C(II)",
        "C3": "C(III)",
        "C4": "C(IV)",
        "D1": "D(I)",
        "D2": "D(II)",
        "D3": "D(III)",
        "PAB": "PA(B)",
        "WOB": "WO(B)"
    }
    if class_of_charge in charge:
        return charge.get(class_of_charge)
    else:
        return class_of_charge

        
        
def class_without_brackets(class_of_charge):
    charge = {
        "C(I)": "C1",
        "C(II)": "C2",
        "C(III)": "C3",
        "C(IV)": "C4",
        "D(I)": "D1",
        "D(II)": "D2",
        "D(III)": "D3",
        "PA(B)": "PAB",
        "WO(B)": "WOB"
    }
    if class_of_charge in charge:
        return charge.get(class_of_charge)
    else:
        return class_of_charge
        
        
def parse_amend_info(info):
    data = {
        'court': None,
        'reference': '',
        'additional_information': ''
    }
    
    match = re.match(r"^(.* COUNTY COURT) NO (\d+ OF \d+)", info)
    if match is not None:
        data['court'] = match.group(1)
        data['reference'] = match.group(2)
        return data
        
    match = re.match(r"^(.* COURT .*) NO (\d+ OF \d+)", info)
    if match is not None:
        data['court'] = match.group(1)
        data['reference'] = match.group(2)
        return data
        
    # RENEWED BY (\d+) DATED (\d\d\/\d\d\/\d{4})
    # RENEWAL OF (\d+) REGD (\d\d\/\d\d\/\d{4})
    # PART CAN (\d+) REGD (\d\d\/\d\d\/\d{4}) SO FAR ONLY AS IT RELATES TO (.*)

    data['additional_information'] = info  # Default fall-back position
    return data
    
    
def reformat_county(county):
    known_variations = {
        'DURHAM': 'COUNTY DURHAM'
    }
    
    if county in known_variations:
        county = known_variations[county]

    match = re.match(r"CITY OF (.*)", county)
    if match is not None:
        c = match.group(1)
        return "{} (city of)".format(c)
    return county
    
    
def save_to_file(data):
    directory = os.path.dirname(__file__)
    directory = os.path.abspath(os.path.join(directory, os.pardir, 'output'))
    
    filename = str(data[0]['registration']['registration_no']) + "_" + data[0]['registration']['date'] + '_' + data[0]['class_of_charge'] + '.txt'
    file = os.path.join(directory, filename)
    
    j = json.dumps(data, sort_keys=True, indent=4)
    with open(file, "w+") as txt:
        txt.write(j)


def extract_authority_name(eo_name):
    # Find text surrounded by + signs. That is the area.
    m = re.search("\+([^\+]+)\+", eo_name)

    result = {
        "name": "",
        "area": ""
    }
    if m is not None:
        result['area'] = m.groups()[0]

    result['name'] = re.sub("\+", " ", eo_name).strip()
    return {'local': result}
