#from application import app, error_queue
from flask import Response, request
import kombu
import json
import logging
import traceback
import threading
import requests
import operator
import re
import time.process_time
from datetime import datetime
from application.utility import convert_class, class_without_brackets, parse_amend_info, save_to_file, reformat_county, \
    extract_authority_name


app_config = None
final_log = []
error_queue = None
wait_time_legacydb = 0
wait_time_landcharges = 0

def is_running():
    threads = [t for t in threading.enumerate() if t.name == 'migrate_thread']
    s = True if len(threads) > 0 and threads[0].is_alive() else False
    return s


def start_migration():
    t = threading.Thread(name='migrate_thread', target=migration_thread)
    t.daemon = False
    t.start()


class MigrationException(RuntimeError):
    def __init__(self, message, text=None):
        super(RuntimeError, self).__init__(message)
        self.text = text


def get_from_legacy_adapter(url, headers={}, params={}):
    start = time.process_time()
    response = requests.get(url, headers=headers, params=params)
    global wait_time_legacydb
    wait_time_legacydb += time.process_time() - start
    return response


def get_registrations_to_migrate(start_date, end_date):
    url = app_config['LEGACY_ADAPTER_URI'] + '/land_charges/' + start_date + '/' + end_date
    headers = {'Content-Type': 'application/json'}
    logging.info("GET %s", url)

    response = get_from_legacy_adapter(url, headers=headers, params={'type': 'NR'})
    logging.info("Responses: %d", response.status_code)
    
    if response.status_code == 200:
        list = response.json()
        logging.info("Found %d items", len(list))
        return list
    else:
        raise MigrationException("Unexpected response {} from {}".format(response.status_code, url))
    # return [{
        # "reg_no": "1416",
        # "date": "2002-04-16",
        # "class": "D2"
    # }]
    # return [{ 
        # "reg_no": "100",
        # "date": "2011-10-10",
        # "class": "PAB"
    # }]

    

# TODO: Important! Can we have duplicate rows on T_LC_DOC_INFO with matching reg number and date???

def get_doc_history(reg_no, class_of_charge, date):
    url = app_config['LEGACY_ADAPTER_URI'] + '/doc_history/' + reg_no
    headers = {'Content-Type': 'application/json'}
    logging.info("  GET %s?class=%s&date=%s", url, class_without_brackets(class_of_charge), date)
    response = get_from_legacy_adapter(url, headers=headers, params={'class': class_without_brackets(class_of_charge), 'date': date})
    logging.info('  Response: %d', response.status_code)
    
    if response.status_code != 200:
        logging.warning("Non-200 return code {} for {}".format(response.status_code, url))

    if response.status_code == 404:
        return None

    return response.json()


def get_land_charge(reg_no, class_of_charge, date):
    url = app_config['LEGACY_ADAPTER_URI'] + '/land_charges/' + str(reg_no)
    headers = {'Content-Type': 'application/json'}
    logging.info('    GET %s?class=%s&date=%s', url, class_of_charge, date)
    response = get_from_legacy_adapter(url, headers=headers, params={'class': class_of_charge, 'date': date})

    logging.info('    Response: %d', response.status_code)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        return None
    else:
        raise MigrationException("Unexpected response {} from {}".format(response.status_code, url),
                                 response.text)


def add_flag(data, flag):
    for item in data:
        item['migration_data']['flags'].append(flag)


def flag_oddities(data):
    # There aren't many circumstances when we can't migrate something - often source data consists only
    # of registration number, date and class of charge (i.e. rest of the data is in the form image)
    # Flag the oddities up anyway so we can check out any data quality issues
    logging.debug("Data:")
    logging.debug(data)
    if data[0]['type'] != 'NR':
        add_flag(data, "Does not start with NR")

    for item in data:
        if item['type'] == 'NR':
            if item != data[0]:
                add_flag(data, "NR is not the first item")
            # if item['migration_data']['original']['registration_no'] != item['registration']['registration_no'] or \
               # item['migration_data']['original']['date'] != item['registration']['date']:
                # add_flag(data, "NR has inconsitent original details")
    
    if len(data[-1]['parties']) == 0:
        add_flag(data, "Last item lacks name information")
                

def log_item_summary(data):
    global final_log
    for item in data:
        final_log.append("Processed " + item['registration']["date"] + "/" + str(item['registration']['registration_no']))
        for flag in item['migration_data']['flags']:
            final_log.append("  " + flag)
               
        
def migrate(config, start, end):
    global app_config
    global error_queue
    app_config = config

    # hostname = "amqp://{}:{}@{}:{}".format(app_config['MQ_USERNAME'], app_config['MQ_PASSWORD'],
    #                                        app_config['MQ_HOSTNAME'], app_config['MQ_PORT'])
    # connection = kombu.Connection(hostname=hostname)
    # error_queue = connection.SimpleQueue('errors')

    logging.info('Migration started')
    error_count = 0
    # Get all the registration numbers that need to be migrated

    try:
        reg_data = get_registrations_to_migrate(start, end)
        if not isinstance(reg_data, list):
            msg = "Registration data is not a list:"
            logging.error(msg)
            logging.error(reg_data)
            report_error("E", msg, json.dumps(reg_data))
            return

        total_read = len(reg_data)
        logging.info("Retrieved %d items from /land_charges", total_read)

    except Exception as e:
        logging.error('Unhandled exception: %s', str(e))
        report_exception(e)
        raise

    total_inc_history = 0
    for rows in reg_data:
        try:
            # For each registration number returned, get history of that application.
            logging.info("------------------------------------------------------------------")
            rows['class'] = rows['class'].strip()
            rows['reg_no'] = rows['reg_no'].strip()
            rows['date'] = rows['date'].strip()
            logging.info("Process %s %s/%s", rows['class'], rows['date'], rows['reg_no'])

            history = get_doc_history(rows['reg_no'], rows['class'], rows['date'])
            if history is None or len(history) == 0:
                logging.error("  No document history information found") # TODO: need a bucket of these
                continue

            total_inc_history += len(history)
            for i in history:
                i['sorted_date'] = datetime.strptime(i['date'], '%Y-%m-%d').date()
            
            logging.info("  Chain of length %d found", len(history))
            history.sort(key=operator.itemgetter('sorted_date', 'reg_no'))
            registration = []
            
            for x, registers in enumerate(history):
                registers['class'] = convert_class(registers['class'])
                
                
                logging.info("    Historical record %s %s %s", registers['class'], registers['reg_no'],
                             registers['date'])

                numeric_reg_no = int(re.sub("/", "", registers['reg_no'])) # TODO: is this safe?
                land_charges = get_land_charge(numeric_reg_no, registers['class'], registers['date'])
                
                if land_charges is not None and len(land_charges) > 0:
                    registration.append(extract_data(land_charges, registers['type']))
                    #registration[x]['reg_no'] = numeric_reg_no
                    
                else:
                    registration.append(build_dummy_row(registers))
                

            flag_oddities(registration)
            
            registration_response = insert_data(registration)
            if registration_response.status_code != 200:
                url = app_config['LAND_CHARGES_URI'] + '/migrated_record'
                message = "Unexpected {} return code for POST {}".format(registration_response.status_code, url)
                logging.error("  " + message)
                report_error("E", message, "")
                logging.error(registration_response.text)

                logging.error("Rows:")
                logging.error(rows)
                logging.error("Registration:")
                logging.error(registration)
                error_count += 1
                item = registration[0]
                final_log.append('Failed to migrate ' + item['registration']["date"] + "/" + str(item['registration']['registration_no']))
            else:
                log_item_summary(registration)
                
            #final_log.append
        except Exception as e:
            logging.error('Unhandled exception: %s', str(e))
            logging.error('Failed to migrate  %s %s %s', rows['class'], rows['reg_no'], rows['date'])
            report_exception(e)
            error_count += 1

    global wait_time_landcharges
    global wait_time_legacydb

    logging.info('Migration complete')
    logging.info("Total registrations read: %d", total_read)
    logging.info("Total records processed: %d", total_inc_history)
    logging.info("Total errors: %d", error_count)
    logging.info("Legacy Adapter wait time: %d", wait_time_legacydb)
    logging.info("Land Charges wait time: %d", wait_time_landcharges)
    
    for line in final_log:
        logging.info(line)


def report_exception(exception):
    global error_queue
    call_stack = traceback.format_exc()
    logging.error(call_stack)
    
    error = {
        "type": "E",
        "message": str(exception),
        "stack": call_stack,
        "subsystem": app_config["APPLICATION_NAME"]
    }
    # TODO: also report exception.text
    # error_queue.put(error)


def report_error(error_type, message, stack):
    global error_queue
    error = {
        "type": error_type,
        "message": message,
        "subsystem": app_config["APPLICATION_NAME"],
        "stack": stack
    }
    error_queue.put(error)


def extract_data(rows, app_type):
    #print(rows)
    data = rows[0]

    if data['reverse_name_hex'][-2:] == '01':
        # County council
        logging.info('      EO Name is County Council')
        registration = build_registration(data, 'County Council', extract_authority_name(data['name']))
    elif data['reverse_name_hex'][-2:] == '02':
        # Rural council
        logging.info('      EO Name is Rural Council')
        registration = build_registration(data, 'Rural Council', extract_authority_name(data['name']))
    elif data['reverse_name_hex'][-2:] == '04':
        # Parish council
        logging.info('      EO Name is Parish Council')
        registration = build_registration(data, 'Parish Council', extract_authority_name(data['name']))
    elif data['reverse_name_hex'][-2:] == '08':
        # Other council
        logging.info('      EO Name is Other Council')
        registration = build_registration(data, 'Other Council', extract_authority_name(data['name']))
    elif data['reverse_name_hex'][-2:] == '16':
        # Dev corp
        logging.info('      EO Name is Development Corporation')
        registration = build_registration(data, 'Development Corporation', {'other': data['name']})
    elif data['reverse_name_hex'][-2:] == 'F1':
        # Ltd Company
        logging.info('      EO Name is Limited Company')
        registration = build_registration(data, 'Limited Company', {'company': data['name']})
    elif data['reverse_name_hex'][-2:] == 'F2':
        # Other
        logging.info('      EO Name is Other')
        registration = build_registration(data, 'Other', {'other': data['name']})
    elif data['reverse_name_hex'][-2:] == 'F3' and data['reverse_name_hex'][0:2] == 'F9':
        logging.info('      EO Name is Complex Name')
        registration = build_registration(data, 'Complex Name', {'complex': {'name': data['name'], 'number': int(data['reverse_name'][2:8], 16)}})
    else:    
        # Mundane name
        logging.info('      EO Name is Simple')
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

    registration = build_registration(rows, 'Private Individual', {'private': {'forenames': forenames, 'surname': surname}})
    return registration

    
def build_dummy_row(entry):
    logging.debug('Entry:')
    logging.debug(entry)
    
    entry = {
        "registration": {
            "registration_no": re.sub("/", "", entry['reg_no']),
            "date": entry['date']
        },
        "parties": [],
        "type": entry['type'],
        "class_of_charge": class_without_brackets(entry['class']),
        "applicant": {'name': '', 'address': '', 'key_number': '', 'reference': ''},
        "additional_information": "",
        "migration_data": {
            'unconverted_reg_no': entry['reg_no'],
            'flags': []
        }       
    }
    
    if entry['class_of_charge'] not in ['PAB', 'WOB']:
        entry['particulars'] = {
            'counties': [],
            'district': '',
            'description': ''
        }
    return entry


def build_registration(rows, name_type, name_data):
    logging.debug('Head Entry:')
    logging.debug(json.dumps(rows))
    
    coc = class_without_brackets(rows['class_type'])
    if coc in ['PAB', 'WOB']:
        eo_type = "Debtor"
        occupation = rows['occupation']
    else:
        eo_type = "Estate Owner"
        occupation = ''
       
    county_text = rows['property_county'].strip()
    
    if county_text == 'BANKS' and coc in ['PA', 'WO', 'DA']: #  Special case for <1% of the data...
        county_text = rows['counties']

    if county_text in ['NO COUNTY', 'NO COUNTIES']:
        county_text = ''
    
    pty_desc = rows['property']
    parish_district = rows['parish_district']
    
    registration = {
        "class_of_charge": coc,
        "registration": {
            "date": rows['registration_date'],
            "registration_no": str(rows['registration_no'])
        },
        "parties": [{
            "type": eo_type,
        }],
        "applicant": {
            'name': '',
            'address': '',
            'key_number': '',
            'reference': ''
        },
        "additional_information": "",
        "migration_data": {
            'unconverted_reg_no': rows['registration_no'],
            'amend_info': rows['amendment_info'],
            'flags': []
        }
    }
    
    amend = parse_amend_info(rows['amendment_info'])
    registration['additional_information'] = amend['additional_information']
    
    if coc in ['PAB', 'WOB']:
        registration['parties'][0]['occupation'] = occupation
        registration['parties'][0]['trading_name'] = ''
        registration['parties'][0]['residence_withheld'] = False
        registration['parties'][0]['case_reference'] = amend['reference']
        registration['parties'][0]['addresses'] = []
        
        address_strings = rows['address'].split('   ')
        for address in address_strings:
            addr_obj = {
                'type': 'Residence',
                'address_string': address            
            }
            registration['parties'][0]['addresses'].append(addr_obj)
        
        
        if amend['court'] is not None:
            registration['parties'].append({
                'type': 'Court',
                'names': [ {
                    'type': 'Other',
                    'other': amend['court']
                } ]
            })       
        
    else:
        registration['particulars'] = {
            'counties': [reformat_county(county_text)],
            'district': parish_district,
            'description': pty_desc
        }

    registration['parties'][0]['names'] = [name_data]
    registration['parties'][0]['names'][0]['type'] = name_type

    return registration


def insert_data(registration):
    json_data = registration

    save_to_file(json_data)
    
    url = app_config['LAND_CHARGES_URI'] + '/migrated_record'
    headers = {'Content-Type': 'application/json'}
    logging.info("  POST %s", url)
    start = time.process_time()
    response = requests.post(url, data=json.dumps(json_data), headers=headers)
    global wait_time_landcharges
    wait_time_landcharges += time.process_time() - start
    logging.info("  Response: %d", response.status_code)
    
    registration_status_code = response
    # add code below to force errors
    # registration_status_code = 500
    return registration_status_code


def hex_translator(hex_code):
    mask = 0x1F
    code_int = int(hex_code, 16)
    length = code_int & mask
    punc_code = code_int >> 5
    punctuation = ['&', ' ', '-', "'", '(', ')', '*']
    return punctuation[punc_code], length

