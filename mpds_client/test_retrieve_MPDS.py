import os.path
import unittest
import httplib2

import numpy as np
import pandas as pd

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
        answer = client.get_data(query, fields={})

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

        for crystal_struct in client.get_data(query, fields={
            'S': ['cell_abc', 'sg_n', 'setting', 'basis_noneq', 'els_noneq']}):

            self.assertEqual(crystal_struct[1], 136)

            ase_obj = MPDSDataRetrieval.compile_crystal(crystal_struct, 'ase')
            if ase_obj:
                self.assertEqual(len(ase_obj), 6)

    def test_get_crystals(self):

        query = {
            "elements": "Ti-O",
            "classes": "binary",
            "props": "atomic structure",
            "sgs": 136
        }
        client = MPDSDataRetrieval()
        ntot = client.count_data(query)
        self.assertTrue(150 < ntot < 175)

        crystals = client.get_crystals(query, flavor='ase')
        for crystal in crystals:
            self.assertIsNotNone(crystal)

        # now try getting the crystal from the phase_id(s)
        phase_ids = {_[0] for _ in client.get_data(query, fields={'S': ['phase_id']})}
        crystals_from_phase_ids = client.get_crystals(query, phases=phase_ids, flavor='ase')

        self.assertEqual(len(crystals), len(crystals_from_phase_ids))

    def test_retrieval_of_phases(self):
        """
        Look for intersection of query_a and query_b
        in two ways:
        maxnphases = changed and maxnphases = default
        """
        query_a = {
            "elements": "O",
            "classes": "binary",
            "props": "band gap"
        }
        query_b = {
            "elements": "O",
            "classes": "binary",
            "props": "isothermal bulk modulus"
        }

        client_one = MPDSDataRetrieval()
        client_one.maxnphases = 50

        answer_one = client_one.get_dataframe(
            query_a,
            fields={'P': ['sample.material.phase_id', 'sample.material.chemical_formula']},
            columns=['Phid', 'Object']
        )
        answer_one = answer_one[np.isfinite(answer_one['Phid'])]
        phases_one = answer_one['Phid'].astype(int).tolist()

        self.assertTrue(len(phases_one) > client_one.maxnphases)

        result_one = client_one.get_dataframe(
            query_b,
            fields={'P': ['sample.material.phase_id', 'sample.material.chemical_formula']},
            columns=['Phid', 'Object'],
            phases=phases_one
        )

        client_two = MPDSDataRetrieval()
        self.assertEqual(client_two.maxnphases, MPDSDataRetrieval.maxnphases)

        answer_two = client_two.get_dataframe(
            query_a,
            fields={'P': ['sample.material.phase_id', 'sample.material.chemical_formula']},
            columns=['Phid', 'Object']
        )
        answer_two = answer_two[np.isfinite(answer_two['Phid'])]
        phases_two = answer_two['Phid'].astype(int).tolist()

        self.assertTrue(len(phases_two) < client_two.maxnphases)

        result_two = client_two.get_dataframe(
            query_b,
            fields={'P': ['sample.material.phase_id', 'sample.material.chemical_formula']},
            columns=['Phid', 'Object'],
            phases=phases_two
        )

        self.assertEqual(len(result_one), len(result_two))

        # check equality of result_one and result_two
        merge = pd.concat([result_one, result_two])
        merge = merge.reset_index(drop=True)
        merge_gpby = merge.groupby(list(merge.columns))
        idx = [x[0] for x in merge_gpby.groups.values() if len(x) == 1]
        self.assertTrue(merge.reindex(idx).empty)

if __name__ == "__main__": unittest.main()
