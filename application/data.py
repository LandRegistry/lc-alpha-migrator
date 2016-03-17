import psycopg2
import psycopg2.extras
import logging
import re
import json
from application.search_key import create_registration_key


app_config = None




# def connect(cursor_factory=None):
    # connection = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(
        # app_config['DATABASE_NAME'], app_config['DATABASE_USER'], app_config['DATABASE_HOST'],
        # app_config['DATABASE_PASSWORD']))
    # return connection.cursor(cursor_factory=cursor_factory)


# def complete(cursor):
    # cursor.connection.commit()
    # cursor.close()
    # cursor.connection.close()


# def rollback(cursor):
    # cursor.connection.rollback()
    # cursor.close()
    # cursor.connection.close()
county_lookup = {}
    

def get_county_id(cursor, county):
    # Minor optimization: ~2% of the SQL insert time reduced
    global county_lookup
    if county in county_lookup:
        return county_lookup[county]


    cursor.execute("SELECT id FROM county WHERE UPPER(name) = %(county)s",
                   {
                       "county": county.upper()
                   })
    rows = cursor.fetchall()
    if len(rows) == 0:
        raise RuntimeError("Invalid county: {}".format(county))
        
    id = rows[0]['id']
    county_lookup[county] = id
    return id


def insert_registration(cursor, details_id, name_id, date, county_id, orig_reg_no=None):
    #logging.debug('Insert registration')
    if orig_reg_no is None:
        # Get the next registration number
        year = date[:4]  # date is a string
        # Optimised: indexing register saves ~1% of SQL insert time
        cursor.execute('select MAX(registration_no) + 1 AS reg '
                       'from register  '
                       'where date >=%(start)s AND date < %(end)s',
                       {
                           'start': "{}-01-01".format(year),
                           'end': "{}-01-01".format(int(year) + 1)
                       })

        rows = cursor.fetchall()
        if rows[0]['reg'] is None:
            reg_no = 1000
        else:
            reg_no = int(rows[0]['reg'])
    else:
        reg_no = orig_reg_no

    # Check if registration_no and date already exist, if they do then increase sequence number
    # TODO: consider if the solution here is actually more robust...
    # cursor.execute('select MAX(reg_sequence_no) + 1 AS seq_no '
                   # 'from register  '
                   # 'where registration_no=%(reg_no)s AND date=%(date)s',
                   # {
                       # 'reg_no': reg_no,
                       # 'date': date
                   # })
    # rows = cursor.fetchall()
    # if rows[0]['seq_no'] is None:
    version = 1
    # else:
        # version = int(rows[0]['seq_no'])

    # Cap it all off with the actual legal "one registration per name":
    cursor.execute("INSERT INTO register (registration_no, debtor_reg_name_id, details_id, date, county_id, reveal, " +
                   "reg_sequence_no) " +
                   "VALUES( %(regno)s, %(debtor)s, %(details)s, %(date)s, %(county)s, 't', %(seq)s ) RETURNING id",
                   {
                       "regno": reg_no,
                       "debtor": name_id,
                       "details": details_id,
                       'date': date,
                       'county': county_id,
                       'seq': version
                   })
    reg_id = cursor.fetchone()[0]
    return reg_no, reg_id


def insert_bankruptcy_regn(cursor, details_id, names, date, orig_reg_no):
    #logging.debug('Inserting banks reg')
    reg_nos = []
    if len(names) == 0:  # Migration case only...
        reg_no, reg_id = insert_registration(cursor, details_id, None, date, None, orig_reg_no)
        reg_nos.append({
            'number': reg_no,
            'date': date,
            'name': None
        })

    else:
        #logging.debug(names)
        for name in names:
            reg_no, reg_id = insert_registration(cursor, details_id, name['id'], date, None, orig_reg_no)
            if 'forenames' in name:
                reg_nos.append({
                    'number': reg_no,
                    'date': date,
                    'forenames': name['forenames'],
                    'surname': name['surname']
                })
            else:
                reg_nos.append({
                    'number': reg_no,
                    'date': date,
                    'name': name['name']
                })
    return reg_nos, reg_id


def insert_landcharge_regn(cursor, details_id, names, county_ids, date, orig_reg_no):
    #logging.debug('Inserting LC reg')
    if len(names) > 1:
        raise RuntimeError("Invalid number of names: {}".format(len(names)))

    reg_nos = []
    if len(county_ids) == 0:  # can occur on migration or a registration against NO COUNTY
        if len(names) > 0:
            name = names[0]['id']
        else:
            name = None

        reg_no, reg_id = insert_registration(cursor, details_id, name, date, None, orig_reg_no)
        reg_nos.append({
            'number': reg_no,
            'date': date,
            'county': None,
        })

    else:
        for county in county_ids:
            #logging.debug(county['id'])
            if len(names) > 0:
                name = names[0]['id']
            else:
                name = None

            reg_no, reg_id = insert_registration(cursor, details_id, name, date, county['id'], orig_reg_no)

            reg_nos.append({
                'number': reg_no,
                'date': date,
                'county': county['name'],
            })

    return reg_nos, reg_id

    
def insert_lc_county(cursor, register_details_id, county):
    #logging.debug('Inserting: ' + county)
    county_id = get_county_id(cursor, county)
    cursor.execute("INSERT INTO detl_county_rel (county_id, details_id) " +
                   "VALUES( %(county_id)s, %(details_id)s ) RETURNING id",
                   {
                       "county_id": county_id, "details_id": register_details_id
                   })
    rows = cursor.fetchall()
    if len(rows) == 0:
        raise RuntimeError("Invalid county ID: {}".format(county))

    return rows[0]['id'], county_id


def insert_counties(cursor, details_id, counties):
    if len(counties) == 1 and (counties[0].upper() == 'NO COUNTY' or counties[0] == ""):
        return []

    ids = []
    for county in counties:
        county_detl_id, county_id = insert_lc_county(cursor, details_id, county)
        ids.append({'id': county_id, 'name': county})
    return ids


def insert_register_details(cursor, request_id, data, date, amends):
    additional_info = data['additional_information'] if 'additional_information' in data else None
    #logging.debug(data)
    priority_notice = None
    if 'particulars' in data:
        district = data['particulars']['district']
        short_description = data['particulars']['description']
        if 'priority_notice' in data['particulars']:
            priority_notice = data['particulars']['priority_notice']
    else:
        district = None
        short_description = None

    debtor = None
    if 'parties' in data:
        for party in data['parties']:
            if party['type'] == 'Debtor':
                debtor = party

    legal_body = None
    legal_ref_no = None

    if debtor is not None:
        legal_ref = debtor['case_reference']
        if 'legal_body' in debtor:
            legal_body = debtor['legal_body']
            legal_ref_no = debtor['legal_body_ref_no']
    else:
        legal_ref = None

    is_priority_notice = None
    prio_notc_expires = None

    #logging.debug(data)
    if 'priority_notice' in data:
        is_priority_notice = True
        # if 'expires' in 'priority_notice':
        prio_notc_expires = data['priority_notice']['expires']
        # else:
        #     prio_notc_expires = data['prio_notice_expires']

    amend_info_type = None
    amend_info_details_orig = None
    amend_info_details_current = None

    amend_type = None
    if 'update_registration' in data:
        update = data['update_registration']
        amend_type = update['type']

        if amend_type == 'Part Cancellation':
            if 'part_cancelled' in update and update['part_cancelled'] != '':
                amend_info_type = 'Part Cancelled'
                amend_info_details_current = update['part_cancelled']
            elif 'plan_attached' in update and update['plan_attached'] != '':
                amend_info_type = 'plan_attached'
                amend_info_details_current = update['plan_attached']
        elif amend_type == 'Rectification':
            if 'instrument' in update and update['instrument'] != '':
                amend_info_type = 'Instrument'
                amend_info_details_orig = update['instrument']['original']
                amend_info_details_current = update['instrument']['current']
            elif 'chargee' in update and update['chargee'] != '':
                amend_info_type = 'Chargee'
                amend_info_details_orig = update['chargee']['original']
                amend_info_details_current = update['chargee']['current']
        elif amend_type == 'Amendment':
            if 'pab' in update and update['pab'] != '':
                amend_info_type = 'PAB'
                amend_info_details_current = update['pab']

    cursor.execute("INSERT INTO register_details (request_id, class_of_charge, legal_body_ref, "
                   "amends, district, short_description, amendment_type, priority_notice_no, "
                   "priority_notice_ind, prio_notice_expires, "
                   "amend_info_type, amend_info_details, amend_info_details_orig ) "
                   "VALUES (%(rid)s, %(coc)s, %(legal_ref)s, %(amends)s, %(dist)s, %(sdesc)s, %(atype)s, "
                   "%(pno)s, %(pind)s, %(pnx)s, %(amd_type)s, "
                   "%(amd_detl_c)s, %(amd_detl_o)s ) "
                   "RETURNING id", {
                       "rid": request_id, "coc": data['class_of_charge'],
                       "legal_ref": legal_ref, "amends": amends, "dist": district,
                       "sdesc": short_description, "atype": amend_type,
                       "pno": priority_notice, 'pind': is_priority_notice, "pnx": prio_notc_expires,
                       "amd_type": amend_info_type, "amd_detl_c": amend_info_details_current,
                       "amd_detl_o": amend_info_details_orig
                   })
    return cursor.fetchone()[0]


def insert_party(cursor, details_id, party):
    occupation = None
    date_of_birth = None
    residence_withheld = False

    if 'occupation' in party:
        occupation = party['occupation']

    if party['type'] == 'Debtor':
        # if 'date_of_birth' in party:
            # date_of_birth = party['date_of_birth']
        # else:
        date_of_birth = None
        residence_withheld = party['residence_withheld']

    cursor.execute("INSERT INTO party (register_detl_id, party_type, occupation, date_of_birth, residence_withheld) " +
                   "VALUES( %(reg_id)s, %(type)s, %(occupation)s, %(dob)s, %(rw)s ) RETURNING id",
                   {
                       "reg_id": details_id, "type": party['type'], "occupation": occupation,
                       "dob": date_of_birth, "rw": residence_withheld
                   })
    return cursor.fetchone()[0]


def insert_address(cursor, address, party_id):
    if 'address_lines' in address and len(address['address_lines']) > 0:
        lines = address['address_lines'][0:5]   # First five lines
        remaining = ", ".join(address['address_lines'][5:])
        if remaining != '':
            lines.append(remaining)             # Remaining lines into 6th line

        while len(lines) < 6:
            lines.append("")                    # Pad to 6 lines for avoidance of horrible if statements later

        county = address['county']
        postcode = address['postcode']       # Postcode in the last
        cursor.execute("INSERT INTO address_detail ( line_1, line_2, line_3, line_4, line_5, line_6 ,county, postcode) "
                       "VALUES( %(line1)s, %(line2)s, %(line3)s, %(line4)s, %(line5)s, %(line6)s, %(county)s, "
                       "%(postcode)s ) RETURNING id",
                       {
                           "line1": lines[0], "line2": lines[1], "line3": lines[2],
                           "line4": lines[3], "line5": lines[4], "line6": lines[5],
                           "county": county, "postcode": postcode,
                       })
        detail_id = cursor.fetchone()[0]
        address_string = "{}, {}, {}".format(", ".join(address['address_lines']), address["county"],
                                             address["postcode"])
    elif 'address_string' in address:
        address_string = address['address_string']
        detail_id = None
    else:
        raise Exception('Invalid address object')

    cursor.execute("INSERT INTO address (address_type, address_string, detail_id) " +
                   "VALUES( %(type)s, %(string)s, %(detail)s ) " +
                   "RETURNING id",
                   {
                       "type": address['type'],
                       "string": address_string,
                       "detail": detail_id
                   })
    address['id'] = cursor.fetchone()[0]

    cursor.execute("INSERT INTO party_address (address_id, party_id) " +
                   "VALUES ( %(address)s, %(party)s ) RETURNING id",
                   {
                       "address": address['id'], "party": party_id
                   })
    return address['id']


def insert_party_name(cursor, party_id, name):
    name_string = None
    forename = None
    middle_names = None
    surname = None
    is_alias = False
    complex_number = None
    complex_name = None
    company = None
    local_auth = None
    local_auth_area = None
    other = None

    if name['type'] == 'Private Individual':
        forename = name['private']['forenames'][0]
        middle_names = ' '.join(name['private']['forenames'][1:])
        surname = name['private']['surname']
        name_string = " ".join(name['private']['forenames']) + " " + name['private']['surname']
    elif name['type'] in ['County Council', 'Rural Council', 'Parish Council', 'Other Council']:
        local_auth = name['local']['name']
        local_auth_area = name['local']['area']
    elif name['type'] in ['Development Corporation', 'Other', 'Coded Name']:
        other = name['other']
    elif name['type'] == 'Limited Company':
        company = name['company']
    elif name['type'] == 'Complex Name':
        complex_number = name['complex']['number']
        complex_name = name['complex']['name']
        #searchable_string = None
    else:
        raise RuntimeError('Unknown name type: {}'.format(name['type']))

    # if name['type'] != 'Complex Name':
    #     searchable_string = get_searchable_string(name_string, company, local_auth, local_auth_area, other)

    # get_searchable_string(name_string=None, company=None, local_auth=None, local_auth_area=None, other=None):
    name_key = create_registration_key(cursor, name)
    cursor.execute("INSERT INTO party_name ( party_name, forename, middle_names, surname, alias_name, "
                   "complex_number, complex_name, name_type_ind, company_name, local_authority_name, "
                   "local_authority_area, other_name, searchable_string, subtype ) "
                   "VALUES ( %(name)s, %(forename)s, %(midnames)s, %(surname)s, %(alias)s, "
                   "%(comp_num)s, %(comp_name)s, %(type)s, %(company)s, "
                   "%(loc_auth)s, %(loc_auth_area)s, %(other)s, %(search_name)s, %(subtype)s ) "
                   "RETURNING id", {
                       "name": name_string, "forename": forename, "midnames": middle_names,
                       "surname": surname, "alias": is_alias, "comp_num": complex_number, "comp_name": complex_name,
                       "type": name['type'], "company": company, "loc_auth": local_auth,
                       "loc_auth_area": local_auth_area, "other": other, "search_name": name_key['key'],
                       'subtype': name_key['indicator']
                   })

    name_id = cursor.fetchone()[0]
    return_data = {
        'id': name_id,
        'name': name
    }

    cursor.execute("INSERT INTO party_name_rel (party_name_id, party_id) " +
                   "VALUES( %(name)s, %(party)s ) RETURNING id",
                   {
                       "name": name_id, "party": party_id
                   })

    return return_data


def insert_details(cursor, request_id, data, date, amends_id):
    #logging.debug("Insert details")
    # register details
    register_details_id = insert_register_details(cursor, request_id, data, date, amends_id)

    debtor_id = None
    debtor = None
    names = []
    for party in data['parties']:
        party_id = insert_party(cursor, register_details_id, party)

        if party['type'] == 'Debtor':
            debtor_id = party_id
            debtor = party
            for address in party['addresses']:
                insert_address(cursor, address, party_id)

        for name in party['names']:
            name_info = insert_party_name(cursor, party_id, name)
            if party['type'] == 'Debtor':
                names.append(name_info)

    # party_trading
    if debtor_id is not None:
        if 'trading_name' in debtor:
            trading_name = debtor['trading_name']
            cursor.execute("INSERT INTO party_trading (party_id, trading_name) " +
                           "VALUES ( %(party)s, %(trading)s ) RETURNING id",
                           {"party": debtor_id, "trading": trading_name})
    return names, register_details_id


def insert_record(cursor, data, request_id, date, amends=None, orig_reg_no=None):

    names, register_details_id = insert_details(cursor, request_id, data, date, amends)

    if data['class_of_charge'] in ['PAB', 'WOB']:
        reg_nos, reg_id = insert_bankruptcy_regn(cursor, register_details_id, names, date, orig_reg_no)
    else:
        county_ids = insert_counties(cursor, register_details_id, data['particulars']['counties'])
        reg_nos, reg_id = insert_landcharge_regn(cursor, register_details_id, names, county_ids, date, orig_reg_no)

    # TODO: audit-log not done. Not sure it belongs here?
    return reg_nos, register_details_id, reg_id


def insert_request(cursor, applicant, application_type, date, original_data=None):
    if original_data is not None:
        cursor.execute("INSERT INTO ins_bankruptcy_request (request_data) VALUES (%(json)s) RETURNING id",
                       {"json": json.dumps(original_data)})
        ins_request_id = cursor.fetchone()[0]
    else:
        ins_request_id = None  # TODO: consider when ins data should be added...

    cursor.execute("INSERT INTO request (key_number, application_type, application_reference, application_date, " +
                   "ins_request_id, customer_name, customer_address) " +
                   "VALUES ( %(key)s, %(app_type)s, %(app_ref)s, %(app_date)s, %(ins_id)s, " +
                   "%(cust_name)s, %(cust_addr)s ) RETURNING id",
                   {
                       "key": applicant['key_number'], "app_type": application_type, "app_ref": applicant['reference'],
                       "app_date": date, "ins_id": ins_request_id, "cust_name": applicant['name'],
                       "cust_addr": applicant['address']
                   })
    return cursor.fetchone()[0]


def mark_as_no_reveal(cursor, reg_no, date):
    cursor.execute("UPDATE register SET reveal=%(rev)s WHERE registration_no=%(regno)s AND date=%(date)s", {
        "rev": False, "regno": reg_no, "date": date
    })


def insert_migration_status(cursor, register_id, registration_number, registration_date, class_of_charge,
                            additional_data):
    cursor.execute("INSERT INTO migration_status (register_id, original_regn_no, date, class_of_charge, "
                   "migration_complete, extra_data ) "
                   "VALUES( %(register_id)s, %(reg_no)s, %(date)s, %(class)s, True, %(extra)s ) RETURNING id",
                   {
                       "register_id": register_id,
                       "reg_no": registration_number,
                       "date": registration_date,
                       "class": class_of_charge,
                       "extra": json.dumps(additional_data)
                   })
    return cursor.fetchone()[0]


def insert_migrated_record(cursor, data):
    logging.info("Insert record")
    data["class_of_charge"] = re.sub(r"\(|\)", "", data["class_of_charge"])

    # TODO: using registration date as request date. Valid? Always?
    types = {
        'NR': 'New registration',
        'AM': 'Amendment',
        'CP': 'Part cancellation',
        'CN': 'Cancellation',
        'RN': 'Renewal',
        'PN': 'Priority notice',
        'RC': 'Rectification'
    }
    type_str = types[data['type']]

    request_id = insert_request(cursor, data['applicant'], type_str, data['registration']['date'], None)

    reg_nos, details_id, reg_id = insert_record(cursor, data, request_id, data['registration']['date'], None,
                                                data['registration']['registration_no'])

    if len(data['parties']) == 0:
        # There was no row on the original index, so by definition it cannot be revealed
        mark_as_no_reveal(cursor, data['registration']['registration_no'], data['registration']['date'])

    insert_migration_status(cursor,
                            reg_id,
                            data['migration_data']['unconverted_reg_no'],
                            data['registration']['date'],
                            data['class_of_charge'],
                            data['migration_data'])

    return details_id, request_id


def get_register_details_id(cursor, reg_no, date):
    cursor.execute("SELECT details_id FROM register WHERE registration_no = %(regno)s AND date=%(date)s " ,
                   "ORDER BY reg_sequence_no DESC "
                   "FETCH FIRST 1 ROW ONLY",
                   {
                       "regno": reg_no,
                       'date': date
                   })
    rows = cursor.fetchall()
    if len(rows) == 0:
        return None
    elif len(rows) > 1:
        raise RuntimeError("Too many rows retrieved")
    else:
        return rows[0]['details_id']


def update_previous_details(cursor, request_id, original_detl_id):
    cursor.execute("UPDATE register_details SET cancelled_by = %(canc)s WHERE " +
                   "id = %(id)s AND cancelled_by IS NULL",
                   {
                       "canc": request_id, "id": original_detl_id
                   })


def insert_migrated_cancellation(cursor, data, index):
    # We've already inserted the predecessor records... this is a 'full' cancellation,
    # So we need to insert the head record, and mark the chain as no-reveal
    cancellation = data[index]
    if len(data) == 1:
        raise RuntimeError("Unexpected length of 1")

    if index == 0:
        raise RuntimeError("Unexpected cancellation at start of chain")

    try:
        logging.info("Insert cancellation")
        predecessor = data[index - 1]

        canc_request_id = insert_request(cursor, cancellation['applicant'], 'Cancellation', cancellation['registration']['date'], None)

        #original_details_id = get_register_details_id(cursor, predecessor['registration']['registration_no'], predecessor['registration']['date'])
        original_details_id = predecessor['details_id']

        canc_date = cancellation['registration']['date']
        reg_nos, canc_details_id, reg_id = insert_record(cursor, cancellation, canc_request_id, canc_date, original_details_id, cancellation['registration']['registration_no'])
                                    # (cursor, data, request_id, date, amends=None, orig_reg_no=None)

        update_previous_details(cursor, canc_details_id, original_details_id)

        for reg in data:
            #logging.debug(reg)
            mark_as_no_reveal(cursor, reg['registration']['registration_no'], reg['registration']['date'])
            logging.info("%s %s hidden", reg['registration']['registration_no'], reg['registration']['date'])
    except Exception as e:
        logging.error(e)
        logging.error('Pre:')
        logging.error(predecessor)
        logging.error('cancel:')
        logging.error(cancellation)
        raise


    return canc_details_id, canc_request_id

    
def connect_to_psql():
    connection = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(
        app_config['DATABASE_NAME'], app_config['DATABASE_USER'], app_config['DATABASE_HOST'],
        app_config['DATABASE_PASSWORD']))
    return connection
    

def disconnect_from_psql(connection):
    connection.close()
    
    
def create_cursor(connection):
    return connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    
def close_cursor(cursor):
    cursor.close()
    
    
def commit(cursor):
    cursor.connection.commit()
    
   
def rollback(cursor):
    cursor.connection.rollback()
    

def migrate_record(config, data):
    # logging.debug("--- MIGRATE RECORD ---")
    # logging.debug(data)


    global app_config
    app_config = config

    previous_id = None
    first_record = data[0]
    failures = []
    conn = None
    
    try:
        conn = connect_to_psql()
    
    
        for register in data:
            
            for index, reg in enumerate(register):
                #cursor = connect(cursor_factory=psycopg2.extras.DictCursor)
                cursor = create_cursor(conn)
            
                try:
                    if reg['type'] == 'CN':
                        details_id, request_id = insert_migrated_cancellation(cursor, register, index)
                        reg['details_id'] = details_id
                    else:
                        details_id, request_id = insert_migrated_record(cursor, reg)
                        reg['details_id'] = details_id

                        if reg['type'] in ['AM', 'CN', 'CP', 'RN', 'RC']:
                            if details_id is not None:
                                cursor.execute("UPDATE register_details SET cancelled_by = %(canc)s WHERE " +
                                               "id = %(id)s AND cancelled_by IS NULL",
                                               {
                                                   "canc": request_id, "id": previous_id
                                               })
                            else:
                                raise RuntimeError("No details ID retrieved: {} {}".format(
                                    reg['registration']['registration_no'],
                                    reg['registration']['date']))

                            # TODO repeating code is bad.
                            if reg['type'] == 'AM':
                                cursor.execute("UPDATE register_details SET amends = %(amend)s, amendment_type=%(type)s WHERE " +
                                               "id = %(id)s",
                                               {
                                                   "amend": previous_id, "id": details_id, "type": "Amendment"
                                               })

                            if reg['type'] == 'RN':
                                cursor.execute("UPDATE register_details SET amends = %(amend)s, amendment_type=%(type)s WHERE " +
                                               "id = %(id)s",
                                               {
                                                   "amend": previous_id, "id": details_id, "type": "Renewal"
                                               })

                            if reg['type'] == 'RC':
                                cursor.execute("UPDATE register_details SET amends = %(amend)s, amendment_type=%(type)s WHERE " +
                                               "id = %(id)s",
                                               {
                                                   "amend": previous_id, "id": details_id, "type": "Rectification"
                                               })

                            if reg['type'] == 'CP':
                                cursor.execute("UPDATE register_details SET amends = %(amend)s, amendment_type=%(type)s WHERE " +
                                               "id = %(id)s",
                                               {
                                                   "amend": previous_id, "id": details_id, "type": "Part Cancellation"
                                               })

                    previous_id = details_id
                    #complete(cursor)
                    commit(cursor)
                except Exception as e:
                    failures.append({
                        'number': reg['registration']['registration_no'],
                        'date': reg['registration']['date'],
                        'message': str(e)
                    })
                    rollback(cursor)
                finally:
                    close_cursor(cursor)
                #raise
    finally:
        if conn is not None:
            disconnect_from_psql(conn)
    
    return failures
