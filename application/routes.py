from application import app, error_queue
from flask import Response, request
import json
import logging
import requests
import operator
from datetime import datetime


@app.route('/', methods=["GET"])
def index():
    return Response(status=200)


@app.route('/begin', methods=["POST"])
def start_migration():
    error = False
    logging.info('Logging invoked: migration started')

    # Get all the registration numbers that need to be migrated. These include cancelled registrations
    # so that a history can be kept.
    url = app.config['B2B_LEGACY_URL'] + '/land_charges'
    headers = {'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        for rows in data:
            # For each registration number returned, get history of that application.
            url = app.config['B2B_LEGACY_URL'] + '/doc_history/' + rows['reg_no']
            headers = {'Content-Type': 'application/json'}
            response = requests.get(url, headers=headers, params={'class': rows['class'], 'date': rows['date']})

            """url = app.config['B2B_LEGACY_URL'] + '/doc_history/' + '604455'
            headers = {'Content-Type': 'application/json'}
            response = requests.get(url, headers=headers, params={'class': 'PA(B)', 'date': '2011-06-12'})"""
            history = response.json()
            # print('history is: ', history)

            for i in history:
                i['sorted_date'] = datetime.strptime(i['date'], '%Y-%m-%d').date()
                i['reg_no'] = int(i['reg_no'])

            history.sort(key=operator.itemgetter('sorted_date', 'reg_no'))
            # print(history)
            registration = []
            for registers in history:
                # print('registers is: ', registers)
                url = app.config['B2B_LEGACY_URL'] + '/land_charges/' + str(registers['reg_no'])
                headers = {'Content-Type': 'application/json'}
                response = requests.get(url, headers=headers,
                                        params={'class': registers['class'], 'date': registers['date']})
                if response.status_code == 200:
                    print('response.json', response.json())
                    registration.append(extract_data(response.json()))
                elif response.status_code != 404:
                    print('error!!', response.status_code)
                else:
                    print('no row found for:', str(registers['reg_no']))

            # print('register_data', registration)
            registration_status_code = insert_data(registration)

            if registration_status_code != 200:
                logging.error("Migration error: %s %s %s", registration_status_code, rows, registration)
                # TODO: process_error("Register Database", registration_status_code, rows, registration)
                error = True

        """
            registration = extract_data(rows)
            registration_status_code = insert_data(registration)

            if registration_status_code != 200:
                logging.error("Migration error: %s %s %s", registration_status_code, rows, registration)
                process_error("Register Database", registration_status_code, rows, registration)
                error = True"""
    else:
        logging.error("Received " + str(response.status_code))
        return Response(status=response.status_code)

    if error is True:
        return Response(status=202, mimetype='application/json')
    else:
        return Response(status=200, mimetype='application/json')


# For testing error queueing:
@app.route('/force_error', methods=['POST'])
def force_error():
    data = request.get_json(force=True)
    row = {
        "registration_no": data['registration_no'],
        "reverse_name": data['reverse_name'],
        "remainder_name": data['remainder_name'],
        "punctuation_code": data['punctuation_code'],
        "class_type": data['class_type']
    }
    registration = {
        "debtor_name": data['debtor_name']
    }
    process_error("Test Errors", "500", row, registration)
    return Response(status=200)


def extract_data(rows):
    data=rows[0]
    # determine the type of extraction needed - simple name/complex name/local authority
    # print('reverse_name', data['reverse_name'])
    if data['reverse_name'][0:2] == 'F9':
        print('we had a complex name', data)
        registration = extract_complex(data)
    elif data['name'] != "":
        registration = extract_authority(data)
    else:
        registration = extract_simple(data)

    return registration


def extract_complex(rows):
    registration = build_registration(rows, None, None, rows['name'])
    # add complex number - this is held in hex form in the 2nd, 3rd and 4th characters of the reverse name
    registration['complex_number'] = int(rows['reverse_name'][2:8], 16)

    return registration


def extract_authority(rows):
    print('we are a local authority', rows)
    registration = build_registration(rows, None, None, rows['name'])
    return registration


def extract_simple(rows):
    hex_codes = []
    length = len(rows['punctuation_code'])
    count = 0
    while count < length:
        hex_codes.append(rows['punctuation_code'][count:(count + 2)])
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

    registration = build_registration(rows, forenames, surname)
    return registration


def build_registration(rows, forenames=None, surname=None, name_string=None):
    if name_string is None:
        name_string = forenames + ' ' + surname
    else:
        forenames = ""
        surname = ""

    registration = {
        "application_type": rows['class_type'],
        "application_ref": rows['amendment_info'],
        "date": rows['registration_date'],
        "debtor_name": {
            "forenames": forenames,
            "surname": surname,
            "name_string": name_string
        },
        "occupation": "",
        "residence": rows['address'],
        "migration_data": {
            "registration_no": rows['registration_no'],
            "extra": {
                "occupation": rows['occupation'],
                "of_note": {
                    "counties": rows['counties'],
                    "property": rows['property'],
                    "parish_district": rows['parish_district'],
                    "priority_notice_ref": rows['priority_notice_ref']
                },
                }
        }
    }
    return registration


def insert_data(registration):
    print('registration = ', registration)
    json_data = registration
    url = app.config['BANKRUPTCY_DATABASE_API'] + '/migrated_record'
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, data=json.dumps(json_data), headers=headers)

    registration_status_code = response.status_code
    # add code below to force errors
    # registration_status_code = 500
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
        "text": ""
    }

    try:
        marker_pos = address.index(marker)
    except ValueError:
        address_1['text'] = address
        address_list.append(address_1.copy())
        return address_list

    while marker_pos > 0:
        address_1['text'] = address[:marker_pos]
        address = address[marker_pos + 3:]
        address_list.append(address_1.copy())
        try:
            marker_pos = address.index(marker)
        except ValueError:
            address_1['text'] = address
            marker_pos = 0
            address_list.append(address_1.copy())
    return address_list


def process_error(database, status_code, rows, registration):
    error_detail = {
        "database": database,
        "status": status_code,
        "registration_no": rows['registration_no'],
        "legacy_name": rows['reverse_name'],
        "legacy_rem_name": rows['remainder_name'],
        "legacy_punc_code": rows['punctuation_code'],
        "class": rows['class_type'],
        "register_name": registration['debtor_name']
    }

    error_queue.write_error(error_detail)
    return
