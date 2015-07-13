from application import app
from flask import Response, request
import json
import logging
import requests


@app.route('/', methods=["GET"])
def index():
    return Response(status=200)


@app.route('/begin', methods=["POST"])
def start_migration():
    if request.headers['Content-Type'] != "application/json":
        return Response(status=415)  # 415 (Unsupported Media Type)

    json_data = request.get_json(force=True)

    url = app.config['B2B_LEGACY_URL'] + '/land_charge'
    headers = {'Content-Type': 'application/json'}
    response = requests.get(url, data=json.dumps(json_data), headers=headers)

    if response.status_code == 200:
        data = response.json
        for rows in data:
            registration = extract_data(rows)
            insert_data(registration)

            if response.status_code == 200:
                return Response(status=200, mimetype='application/json')
            else:
                logging.error("Received " + str(response.status_code))
                return Response(status=response.status_code)
    else:
        logging.error("Received " + str(response.status_code))
        return Response(status=response.status_code)


def extract_data(rows):
    hex_codes = []
    length = len(rows['punctuation_code'])
    count = 0
    while count < length:
        hex_codes.append(rows['punctuation_code'][count:(count+2)])
        count += 2

    orig_name = rows["rem_name"] + rows["name"][::-1]
    print("orig_name: ", orig_name)
    name_list = []
    for items in hex_codes:
        punc, pos = hex_translator(items)
        name_list.append(orig_name[:pos])
        name_list.append(punc)
        orig_name = orig_name[pos:]

    name_list.append(orig_name)
    full_name = ''.join(name_list)
    surname_pos = full_name.index('*')
    forename = full_name[:surname_pos]
    surname = full_name[surname_pos + 1:]
    print(forename, surname)

    registration = {
        "key_number": "2244095",
        "application_type": rows['class_type'],
        "application_ref": " ",
        "date": rows['registration_date'],
        "debtor_name": {
            "forename": [forename],
            "surname": surname
        },
        "debtor_alternative_name": [],
        "occupation": rows['occupation'],
        "residence": [{
            "address_lines": [
                rows['address']
            ],
            "postcode": " "
            }
        ],
        "residence_withheld": False,
        "date_of_birth": " ",
        "investment_property": []
    }
    return registration


def insert_data(registration):
    json_data = registration
    url = app.config['BANKRUPTCY_DATABASE_API'] + '/register'
    headers = {'Content-Type': 'application/json'}
    response = requests.get(url, data=json.dumps(json_data), headers=headers)

    return Response(status=response.status_code)


def hex_translator(hex_code):
    compare_bit = 0x1F
    compare_int = int(compare_bit)
    myint = int(hex_code, 16)
    int_3 = myint >> 5
    bit_3 = bin(int_3)
    diff = compare_int & myint
    diff_bit = (bin(diff))
    int_5 = int(diff_bit, 2)
    punctuation = {
        "0b1": " ",
        "0b10": "-",
        "0b11": "'",
        "0b100": "(",
        "0b101": "(",
        "0b110": "*",
        "0b0": "&"
    }

    print("punctuation character is:", punctuation[str(bit_3)])
    print("position in string is:", int_5)
    return punctuation[str(bit_3)], int_5