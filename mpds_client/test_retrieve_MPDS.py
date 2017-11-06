
import os.path
import unittest
import httplib2

import ujson as json
from jsonschema import validate, Draft4Validator
from jsonschema.exceptions import ValidationError

from retrieve_MPDS import MPDSDataRetrieval


class MPDSDataRetrievalTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        network = httplib2.Http()
        response, content = network.request('http://developer.mpds.io/mpds.schema.json')
        assert response.status == 200

        cls.schema = json.loads(content)
        Draft4Validator.check_schema(cls.schema)

    def test_valid_answer(self):

        query = {
            "elements": "K-Ag",
            "classes": "iodide",
            "props": "heat capacity",
            "lattices": "cubic"
        }

        client = MPDSDataRetrieval()
        answer = client.get_data(query, fields = {})

        try:
            validate(answer, self.schema)
        except ValidationError as e:
            self.fail(
                "The item: \r\n\r\n %s \r\n\r\n has an issue: \r\n\r\n %s" % (
                    e.instance, e.context
                )
            )

    def test_crystal_structure(self):

        query = {
            "elements": "Ti-O",
            "classes": "binary",
            "props": "atomic structure",
            "sgs": 136
        }

        client = MPDSDataRetrieval()
        ntot = client.count_data(query)
        self.assertTrue(150 < ntot < 175)

        for crystal_struct in client.get_data(query, fields = {'S':['cell_abc', 'sg_n', 'setting', 'basis_noneq', 'els_noneq']}):

            self.assertEqual(crystal_struct[1], 136)

            ase_obj = MPDSDataRetrieval.compile_crystal(crystal_struct, 'ase')
            if not ase_obj: continue

            self.assertEqual(len(ase_obj), 6)

if __name__ == "__main__": unittest.main()
