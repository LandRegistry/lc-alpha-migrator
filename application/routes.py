from application import app, error_queue
from flask import Response, request
import json
import logging
import requests


@app.route('/', methods=["GET"])
def index():
    return Response(status=200)


@app.route('/begin', methods=["POST"])
def start_migration():
    error_list = []
    error = False
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    url = app.config['B2B_LEGACY_URL'] + '/land_charge?' + 'start_date=' + start_date + '&' + 'end_date=' + end_date
    headers = {'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        for rows in data:
            registration = extract_data(rows)
            registration_status_code = insert_data(registration)

            if registration_status_code != 200:
                """logging.error("Received " + str(registration_status_code))
                return Response(status=registration_status_code)"""
                process_error("Register Database", registration_status_code, rows, registration)
                error = True
    else:
        logging.error("Received " + str(response.status_code))
        return Response(status=response.status_code)

    if error is True:
        while True:
            try:
                message_read = error_queue.read_error()
                error_list.append(message_read)
            except Exception:
                break
        print(error_list)
        return Response(json.dumps(error_list), status=202, mimetype='application/json')
    else:
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
    try:
        surname_pos = full_name.index('*')
        forenames = full_name[:surname_pos]
        surname = full_name[surname_pos + 1:]
    except ValueError:
        surname = ""
        forenames = full_name

    forenames = forenames.split()
    addresses = extract_address(rows['address'])

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
        "residence": addresses,
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

    # registration_status_code = response.status_code
    registration_status_code = 500
    return registration_status_code


def hex_translator(hex_code):
    compare_bit = 0x1F
    compare_int = int(compare_bit)
    myint = int(hex_code, 16)
    int_3 = myint >> 5
    bit_3 = bin(int_3)
    diff = compare_int & myint
    diff_bit = (bin(diff))
    dec_5 = int(diff_bit, 2)
    punctuation = {
        "0b1": " ",
        "0b10": "-",
        "0b11": "'",
        "0b100": "(",
        "0b101": ")",
        "0b110": "*",
        "0b0": "&"
    }

    return punctuation[str(bit_3)], dec_5


def extract_address(address):
    marker = "   "
    address_list = []
    address_1 = {
        "address_lines": [],
        "postcode": ""
    }

    try:
        marker_pos = address.index(marker)
    except ValueError:
        address_1['address_lines'].insert(0, address)
        address_list.append(address_1.copy())
        return address_list

    while marker_pos > 0:
        address_1['address_lines'].insert(0, address[:marker_pos])
        address = address[marker_pos + 3:]
        address_list.append(address_1.copy())
        address_1['address_lines'] = []
        try:
            marker_pos = address.index(marker)
        except ValueError:
            address_1['address_lines'].insert(0, address)
            marker_pos = 0
            address_list.append(address_1.copy())

    return address_list


def process_error(db, status_code, rows, registration):
    error_detail = {
        "registration_no": rows['registration_no'],
        "legacy_name": rows['reverse_name'],
        "legacy_rem_name": rows['remainder_name'],
        "legacy_punc_code": rows['punctuation_code'],
        "class": rows['class_type'],
        "register_name": registration['debtor_name']
        }

    error_message = "Call to " + db + " with code " + str(status_code) + ". Details: " + str(error_detail)
    error_queue.write_error(error_message)
    return