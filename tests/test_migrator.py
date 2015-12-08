import pytest
from unittest import mock
from application.routes import app, extract_data, hex_translator, extract_address
import requests
import json


class FakeResponse(requests.Response):
    def __init__(self, status_code=200):
        super(FakeResponse, self).__init__()

        self._content_consumed = True
        self.status_code = status_code

    def json(self):
        # sets up the json required in the mock response
        data = [
            {
                "time": "2014-09-02 20:01:45.504423",
                "registration_no": "2342",
                "priority_notice": " ",
                "reverse_name": "HCLEWREDNAZX",
                "property_county": 0,
                "registration_date": "2014-12-28",
                "class_type": "WO(B)",
                "remainder_name": "GUYUBALDOALE",
                "punctuation_code": "2326CA",
                "name": " ",
                "address": "9320 KAREEM LOCK JACOBSSIDE EAST HARRYLAND OA34 7BC CUMBRIA",
                "occupation": "CARPENTER",
                "counties": "",
                "amendment_info": "MCLAUGHLINTOWN COUNTY COURT 805 OF 2015",
                "property": "",
                "parish_district": "",
                "priority_notice_ref": "",
                "reg_no": "12345",
                "class": "C1",
                "date": "2011-06-01",
                "type": "C1"
            }
        ]
        return data


test_data = [
    {
        "input": [{
            "time": "2014-09-02 20:01:45.504423",
            "registration_no": "2342",
            "priority_notice": " ",
            "reverse_name": "EERBOREDNAZX",
            "property_county": 0,
            "registration_date": "2014-12-28",
            "class_type": "WO(B)",
            "remainder_name": "GUYUBALDOALE",
            "punctuation_code": "2326CA61",
            "name": "",
            "address": "9320 KAREEM LOCK EAST HARRYLAND OA34 7BC CUMBRIA   23 WILLIAM PRANCE ROAD, PLYMOUTH",
            "occupation": "CARPENTER",
            "counties": "",
            "amendment_info": "MCLAUGHLINTOWN COUNTY COURT 805 OF 2015",
            "property": "",
            "parish_district": "",
            "priority_notice_ref": ""
        }],
        "expected": {
            "application_type": "WO(B)",
            "application_ref": "MCLAUGHLINTOWN COUNTY COURT 805 OF 2015",
            "date": "2014-12-28",
            "debtor_name": {
                "forenames": ["GUY", "UBALDO", "ALEXZANDER"],
                "surname": "O'BREE"
            },
            "occupation": "",
            "residence":
                {"text": "9320 KAREEM LOCK EAST HARRYLAND OA34 7BC CUMBRIA   23 WILLIAM PRANCE ROAD, PLYMOUTH"},
            "migration_data": {
                "registration_no": "2342",
                "extra": {
                    "occupation": "CARPENTER",
                    "of_note": {
                        "counties": "",
                        "property": "",
                        "parish_district": "",
                        "priority_notice_ref": ""
                    },
                }
            }
        }
    }, {
        "input": [{
            "time": "2014-09-02 20:01:45.504423",
            "registration_no": "2342",
            "priority_notice": " ",
            "reverse_name": "HCLEWREDNAZX",
            "property_county": 0,
            "registration_date": "2014-12-28",
            "class_type": "WO(B)",
            "remainder_name": "GUYUBALDOALE",
            "punctuation_code": "23262A",
            "name": "",
            "address": "9320 KAREEM LOCK EAST HARRYLAND OA34 7BC CUMBRIA   23 WILLIAM PRANCE ROAD, PLYMOUTH",
            "occupation": "CARPENTER",
            "counties": "",
            "amendment_info": "MCLAUGHLINTOWN COUNTY COURT 805 OF 2015",
            "property": "",
            "parish_district": "",
            "priority_notice_ref": ""
        }],
        "expected": {
            "application_type": "WO(B)",
            "application_ref": "MCLAUGHLINTOWN COUNTY COURT 805 OF 2015",
            "date": "2014-12-28",
            "debtor_name": {
                "forenames": ["GUY", "UBALDO", "ALEXZANDER", "WELCH"],
                "surname": ""
            },
            "occupation": "",
            "residence":
                {"text": "9320 KAREEM LOCK EAST HARRYLAND OA34 7BC CUMBRIA   23 WILLIAM PRANCE ROAD, PLYMOUTH"},
            "migration_data": {
                "registration_no": "2342",
                "extra": {
                    "occupation": "CARPENTER",
                    "of_note": {
                        "counties": "",
                        "property": "",
                        "parish_district": "",
                        "priority_notice_ref": ""
                    },
                }
            }
        }
    },
]

complex_data = {
    "input": [{
        "time": "2014-09-02 20:01:45.504423",
        "registration_no": "2342",
        "priority_notice": " ",
        "reverse_name": "F91041E800000000000000F3",
        "property_county": 0,
        "registration_date": "2014-12-28",
        "class_type": "WO(B)",
        "remainder_name": "",
        "punctuation_code": "",
        "name": "BORERBERG VISCOUNT",
        "address": "9320 KAREEM LOCK EAST HARRYLAND OA34 7BC CUMBRIA   23 WILLIAM PRANCE ROAD, PLYMOUTH",
        "occupation": "CARPENTER",
        "counties": "",
        "amendment_info": "MCLAUGHLINTOWN COUNTY COURT 805 OF 2015",
        "property": "",
        "parish_district": "",
        "priority_notice_ref": ""
    }],
    "expected": {
        "application_type": "WO(B)",
        "application_ref": "MCLAUGHLINTOWN COUNTY COURT 805 OF 2015",
        "date": "2014-12-28",
        "debtor_name": {
            "forenames": ["GUY", "UBALDO", "ALEXZANDER", "WELCH"],
            "surname": ""
        },
        "complex": {"name": "BORERBERG VISCOUNT",
                    "number": 1065448},
        "occupation": "",
        "residence":
            {"text": "9320 KAREEM LOCK EAST HARRYLAND OA34 7BC CUMBRIA   23 WILLIAM PRANCE ROAD, PLYMOUTH"},
        "migration_data": {
            "registration_no": "2342",
            "extra": {
                "occupation": "CARPENTER",
                "of_note": {
                    "counties": "",
                    "property": "",
                    "parish_district": "",
                    "priority_notice_ref": ""
                },
                }
        }
    }
}

test_hex = [
    {
        "input": "10",
        "expected": ["&", 10]
    }, {
        "input": "23",
        "expected": [" ", 3]
    }, {
        "input": "47",
        "expected": ["-", 7]
    }, {
        "input": "6A",
        "expected": ["'", 10]
    }, {
        "input": "83",
        "expected": ["(", 3]
    }, {
        "input": "AC",
        "expected": [")", 12]
    }, {
        "input": "DB",
        "expected": ["*", 28]
    }
]

test_address = [
    {
        "input": "23 Tavistock Road, Derriford, Plymouth  PL3 9TT   45 High Street, Saltash, Cornwall   1 Road, "
                 "Plymouth",
        "expected": [
            {"text": "23 Tavistock Road, Derriford, Plymouth  PL3 9TT"},
            {"text": "45 High Street, Saltash, Cornwall"},
            {"text": "1 Road Plymouth"}
        ]
    }, {
        "input": "Various addresses in  Dorsert and  Devon  which can be supplied on request",
        "expected": [
            {"text": "Various addresses in  Dorsert and  Devon  which can be supplied on request"}
        ]
    }, {
        "input": "23 My Road, Town Centre, Postcode City 45 Middle Road, Midtown, City",
        "expected": [
            {"text": "23 My Road, Town Centre, Postcode City 45 Middle Road, Midtown, City"}
        ]
    }
]


class TestMigrationProcess:
    def setup_method(self, method):
        self.app = app.test_client()

    def test_reality(self):
        assert 1 + 2 == 3

    def test_healthcheck(self):
        response = self.app.get("/")
        assert response.status_code == 200

    def test_notfound(self):
        response = self.app.get("/doesnt_exist")
        assert response.status_code == 404

    fake_auto_success = FakeResponse(200)
    fake_auto_fail = FakeResponse(400)

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_migrate(self, mock_post, mock_get):
        data = test_data[0]
        extracted = extract_data(data['input'], 'C1')
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert extracted['debtor_name'] == data['expected']['debtor_name']
        assert extracted['residence'] == data['expected']['residence']

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_hex_convertor_ampersand(self, mock_post, mock_get):
        data = test_hex[0]
        hexconvert = hex_translator(data['input'])
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert hexconvert[0] == data['expected'][0]

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_hex_convertor_blank(self, mock_post, mock_get):
        data = test_hex[1]
        hexconvert = hex_translator(data['input'])
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert hexconvert[0] == data['expected'][0]

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_hex_convertor_hyphen(self, mock_post, mock_get):
        data = test_hex[2]
        hexconvert = hex_translator(data['input'])
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert hexconvert[0] == data['expected'][0]

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_hex_convertor_apos(self, mock_post, mock_get):
        data = test_hex[3]
        hexconvert = hex_translator(data['input'])
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert hexconvert[0] == data['expected'][0]

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_hex_convertor_lbracket(self, mock_post, mock_get):
        data = test_hex[4]
        hexconvert = hex_translator(data['input'])
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert hexconvert[0] == data['expected'][0]

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_hex_convertor_rbracket(self, mock_post, mock_get):
        data = test_hex[5]
        hexconvert = hex_translator(data['input'])
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert hexconvert[0] == data['expected'][0]

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_hex_convertor_asterisk(self, mock_post, mock_get):
        data = test_hex[6]
        hexconvert = hex_translator(data['input'])
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert hexconvert[0] == data['expected'][0]

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_fail)
    def test_legacy_fail(self, mock_post, mock_get):
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 400

    @mock.patch('requests.post', return_value=fake_auto_fail)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_register_fail(self, mock_post, mock_get):
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 202

    # test that multiple addresses supplied with 3 blank separation format correctly
    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_3_addresses(self, mock_post, mock_get):
        data = test_address[0]
        extracted = extract_address(data['input'])
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert extracted[0] == data['expected'][0]

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_address_2_blanks(self, mock_post, mock_get):
        data = test_address[1]
        extracted = extract_address(data['input'])
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert extracted[0] == data['expected'][0]

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_addresses_string(self, mock_post, mock_get):
        data = test_address[2]
        extracted = extract_address(data['input'])
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert extracted[0] == data['expected'][0]

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_no_surname(self, mock_post, mock_get):
        data = test_data[1]
        extracted = extract_data(data['input'], "C4")
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert extracted['debtor_name'] == data['expected']['debtor_name']
        assert extracted['residence'] == data['expected']['residence']

    @mock.patch('requests.post', return_value=fake_auto_success)
    @mock.patch('requests.get', return_value=fake_auto_success)
    def test_complex_number(self, mock_post, mock_get):
        data = complex_data
        extracted = extract_data(data['input'], "WO")
        headers = {'Content-Type': 'application/json'}
        response = self.app.post('/begin', headers=headers)
        assert response.status_code == 200
        assert extracted['complex'] == data['expected']['complex']
