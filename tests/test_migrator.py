import pytest
#from unittest import mock
#from application.routes import app, extract_data, hex_translator, extract_address
from application.routes import extract_data
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
    def test_encode_name(self):
        assert False