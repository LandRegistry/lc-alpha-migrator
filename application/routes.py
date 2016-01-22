from application import app, error_queue
from flask import Response, request
import json
import logging
import traceback
import threading
import requests
import operator
from datetime import datetime


@app.route('/', methods=["GET"])
def index():
    return Response(status=200)


@app.route('/status', methods=['GET'])
def get_status():
    s = "running" if is_running() else "idle"
    return Response(json.dumps({"status": s}), status=200)


@app.route('/begin', methods=["POST"])
def start_migration():
    if not is_running():
        start_migration()
        return Response(status=200)
    else:
        return Response(status=400)


def is_running():
    threads = [t for t in threading.enumerate() if t.name == 'migrate_thread']
    s = True if len(threads) > 0 and threads[0].is_alive() else False
    return s


def start_migration():
    t = threading.Thread(name='migrate_thread', target=migration_thread)
    t.daemon = False
    t.start()


def migration_thread():
    error = False
    logging.info('Migration started')
    error_count = 0
    # Get all the registration numbers that need to be migrated

    if False:
        url = app.config['B2B_LEGACY_URL'] + '/land_charges'
        headers = {'Content-Type': 'application/json'}
        response = requests.get(url, headers=headers, params={'type': 'NR'})

        if response.status_code == 200:
            reg_data = response.json()
            if not isinstance(reg_data, list):
                msg = "Response from {} is not a list.".format(url)
                logging.error(msg)
                logging.error(reg_data)
                report_error("E", msg, json.dumps(reg_data))
                return
        else:
            msg = "Received %d from %s", response.status_code, url
            logging.error(msg)
            report_error("E", msg, "")
            return
    else:
        reg_data = [{
            'class': 'C4', 'reg_no': '30563', 'date': '1991-07-05'
        }]

    total_read = len(reg_data)
    logging.info("Retrieved %d items from /land_charges", total_read)
    total_inc_history = 0

    for rows in reg_data:

        try:
            # For each registration number returned, get history of that application.
            logging.info("Process %s %s %s", rows['class'], rows['reg_no'], rows['date'])
            url = app.config['B2B_LEGACY_URL'] + '/doc_history/' + rows['reg_no']
            headers = {'Content-Type': 'application/json'}
            response = requests.get(url, headers=headers, params={'class': rows['class'], 'date': rows['date']})

            history = response.json()
            total_inc_history += len(history)

            for i in history:
                i['sorted_date'] = datetime.strptime(i['date'], '%Y-%m-%d').date()
                i['reg_no'] = int(i['reg_no'])

            history.sort(key=operator.itemgetter('sorted_date', 'reg_no'))
            registration = []
            for x, registers in enumerate(history):
                registers['class'] = convert_class(registers['class'])
                logging.info("  > Historical record %s %s %s", registers['class'], registers['reg_no'],
                             registers['date'])

                url = app.config['B2B_LEGACY_URL'] + '/land_charges/' + str(registers['reg_no'])
                headers = {'Content-Type': 'application/json'}
                response = requests.get(url, headers=headers,
                                        params={'class': registers['class'], 'date': registers['date']})
                if response.status_code == 200:
                    registration.append(extract_data(response.json(), registers['type']))
                    registration[x]['reg_no'] = registers['reg_no']
                elif response.status_code == 404:
                    del registers['sorted_date']
                    registers['application_type'] = registers['class']
                    registers['application_ref'] = ' '
                    registers['migration_data'] = {"registration_no": registers['reg_no'],
                                                   "extra": {}}
                    registers['residence'] = {"text": ""}
                    registration.append(registers)
                else:
                    message = "Unexpected {} return code for GET {}".format(response.status_code, url)
                    logging.error("  > " + message)
                    error_count += 1
                    report_error("E", message, response.text)

            registration_status_code = insert_data(registration)

            if registration_status_code != 200:
                url = app.config['BANKRUPTCY_DATABASE_API'] + '/migrated_record'
                message = "Unexpected {} return code for POST {}".format(registration_status_code, url)
                logging.error("  > " + message)
                report_error("E", message, "")

                logging.error("Rows:")
                logging.error(rows)
                logging.error("Registration:")
                logging.error(registration)
                error_count += 1
        except Exception as e:
            logging.error('Unhandled exception: %s', str(e))
            report_exception(e)
            pass


    logging.info('Migration complete')
    logging.info("Total registrations read: %d", total_read)
    logging.info("Total records processed: %d", total_inc_history)
    logging.info("Total errors: %d", error_count)


def report_exception(exception):
    call_stack = traceback.format_exc()
    error = {
        "type": "E",
        "message": str(exception),
        "stack": call_stack,
        "subsystem": app.config["APPLICATION_NAME"]
    }
    error_queue.write_error(error)


def report_error(error_type, message, stack):
    error = {
        "type": error_type,
        "message": message,
        "subsystem": app.config["APPLICATION_NAME"],
        "stack": stack
    }
    error_queue.write_error(error)


# For testing error queueing:
@app.route('/force_error', methods=['POST'])
def force_error():
    report_error("I", "Test Error", "Stack goes here")
    return Response(status=200)


def extract_data(rows, app_type):
    data = rows[0]
    # determine the type of extraction needed - simple name/complex name/local authority
    if data['reverse_name'][0:2] == 'F9':
        print('we had a complex name', data['reverse_name'])
        registration = build_registration(data, None, None, {'name': data['name'],
                                                             'number': int(data['reverse_name'][2:8], 16)})
    elif data['name'] != "":
        registration = build_registration(data, None, None, {'name': data['name'], 'number': 0})
    else:
        registration = extract_simple(data)

    registration['type'] = app_type
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


def build_registration(rows, forenames=None, surname=None, complex_data=None):

    registration = {
        "application_type": rows['class_type'],
        "application_ref": rows['amendment_info'],
        "date": rows['registration_date'],
        "occupation": "",
        "residence": {"text": rows['address']},
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
    if complex_data is None:
        registration['debtor_name'] = {"forenames": forenames, "surname": surname}
    else:
        registration['complex'] = complex_data
        registration['debtor_name'] = {"forenames": [""], "surname": ""}

    return registration


def insert_data(registration):
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
    logging.warning("Deprecated method: process_error")
    error_detail = {
        "database": database,
        "status": status_code,
        "registration_no": rows['registration_no'],
        "legacy_name": rows['reverse_name'],
        "legacy_rem_name": rows['remainder_name'],
        "legacy_punc_code": rows['punctuation_code'],
        "class": rows['class_type'],
        "register_name": registration[0]['debtor_name']
    }

    error_queue.write_error(error_detail)
    return
