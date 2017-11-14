
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

        for crystal_struct in client.get_data(query, fields={'S':['cell_abc', 'sg_n', 'setting', 'basis_noneq', 'els_noneq']}):

            self.assertEqual(crystal_struct[1], 136)

            ase_obj = MPDSDataRetrieval.compile_crystal(crystal_struct, 'ase')
            if not ase_obj: continue

            self.assertEqual(len(ase_obj), 6)

    def test_retrieval_of_phases(self):
        """
        Look for intersection of query_a and query_b
        in two ways:
        maxnphases = 10 and maxnphases = default
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

        origv = MPDSDataRetrieval.maxnphases
        MPDSDataRetrieval.maxnphases = 25
        client_one = MPDSDataRetrieval()

        answer = client_one.get_dataframe(
            query_a,
            fields={'P': ['sample.material.phase_id', 'sample.material.chemical_formula']},
            columns=['Phid', 'Object']
        )
        answer = answer[np.isfinite(answer['Phid'])]
        result_one = client_one.get_dataframe(
            query_b,
            fields={'P': ['sample.material.phase_id', 'sample.material.chemical_formula']},
            columns=['Phid', 'Object'],
            phases=answer['Phid'].astype(int).tolist()
        )

        MPDSDataRetrieval.maxnphases = origv
        client_two = MPDSDataRetrieval()

        answer = client_two.get_dataframe(
            query_a,
            fields={'P': ['sample.material.phase_id', 'sample.material.chemical_formula']},
            columns=['Phid', 'Object']
        )
        answer = answer[np.isfinite(answer['Phid'])]
        result_two = client_two.get_dataframe(
            query_b,
            fields={'P': ['sample.material.phase_id', 'sample.material.chemical_formula']},
            columns=['Phid', 'Object'],
            phases=answer['Phid'].astype(int).tolist()
        )

        # get df difference and assure in none
        merge = pd.concat([result_one, result_two])
        merge = merge.reset_index(drop=True)
        merge_gpby = merge.groupby(list(merge.columns))
        idx = [x[0] for x in merge_gpby.groups.values() if len(x) == 1]
        self.assertTrue(merge.reindex(idx).empty)

if __name__ == "__main__": unittest.main()
