from application import app
from flask import Response, request
import json
import logging
import requests


@app.route('/', methods=["GET"])
def index():
    print("migrator called")
    return Response(status=200)


@app.route('/begin', methods=["POST"])
def start_migration():
    print("start_migration called")
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    url = app.config['B2B_LEGACY_URL'] + '/land_charge?' + 'start_date=' + start_date + '&' + 'end_date=' + end_date
    print(url)
    headers = {'Content-Type': 'application/json'}
    print("calling legacy url")
    response = requests.get(url, headers=headers)

    print(response.status_code)
    if response.status_code == 200:
        data = response.json()
        print(data)
        for rows in data:
            registration = extract_data(rows)
            registration_status_code = insert_data(registration)

            if registration_status_code != 200:
                logging.error("Received " + str(registration_status_code))
                return Response(status=registration_status_code)
    else:
        logging.error("Received " + str(response.status_code))
        return Response(status=response.status_code)

    return Response(status=200, mimetype='application/json')


def extract_data(rows):
    hex_codes = []
    length = len(rows['punctuation_code'])
    count = 0
    while count < length:
        hex_codes.append(rows['punctuation_code'][count:(count+2)])
        count += 2

    orig_name = rows["remainder_name"] + rows["reverse_name"][::-1]
    name_list = []
    for items in hex_codes:
        punc, pos = hex_translator(items)
        name_list.append(orig_name[:pos])
        name_list.append(punc)
        orig_name = orig_name[pos:]

    name_list.append(orig_name)
    full_name = ''.join(name_list)
    surname_pos = full_name.index('*')
    forenames = full_name[:surname_pos]
    surname = full_name[surname_pos + 1:]
    forenames = forenames.split()

    registration = {
        "key_number": "2244095",
        "application_type": rows['class_type'],
        "application_ref": " ",
        "date": rows['registration_date'],
        "debtor_name": {
            "forenames": forenames,
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
        "date_of_birth": "1975-10-07",
        "investment_property": []
    }
    return registration


def insert_data(registration):
    json_data = registration
    url = app.config['BANKRUPTCY_DATABASE_API'] + '/register'
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, data=json.dumps(json_data), headers=headers)

    registration_status_code = response.status_code
    return registration_status_code


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