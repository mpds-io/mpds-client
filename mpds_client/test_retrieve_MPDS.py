import unittest
# import warnings

import polars as pl

import httplib2
import ujson as json
from jsonschema import validate, Draft4Validator
from jsonschema.exceptions import ValidationError

from retrieve_MPDS import MPDSDataRetrieval
import logging


class MPDSDataRetrievalTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

        network = httplib2.Http()
        response, content = network.request(
            "https://developer.mpds.io/mpds.schema.json"
        )
        assert response.status == 200

        cls.schema = json.loads(content)
        Draft4Validator.check_schema(cls.schema)

    def test_valid_answer(self):
        query = {
            "elements": "K-Ag",
            "classes": "iodide",
            "props": "heat capacity",
            "lattices": "cubic",
        }

        client = MPDSDataRetrieval()
        answer = client.get_data(query, fields={})

        try:
            validate(answer, self.schema)
        except ValidationError as e:
            self.fail(
                "The item: \r\n\r\n %s \r\n\r\n has an issue: \r\n\r\n %s"
                % (e.instance, e.context)
            )

    def test_crystal_structure(self):
        query = {
            "elements": "Ti-O",
            "classes": "binary",
            "props": "atomic structure",
            "sgs": 136,
        }

        client = MPDSDataRetrieval()
        ntot = client.count_data(query)
        self.assertTrue(90 < ntot < 200)

        for crystal_struct in client.get_data(
            query, fields={"S": ["cell_abc", "sg_n", "basis_noneq", "els_noneq"]}
        ):
            self.assertEqual(crystal_struct[1], 136)

            ase_obj = MPDSDataRetrieval.compile_crystal(crystal_struct, "ase")
            if ase_obj:
                self.assertEqual(len(ase_obj), 6)

    def test_get_crystals(self):
        query = {
            "elements": "Ti-O",
            "classes": "binary",
            "props": "atomic structure",
            "sgs": 136,
        }
        client = MPDSDataRetrieval()
        ntot = client.count_data(query)
        logging.debug(f"Value of ntot: {ntot}")
        self.assertTrue(190 < ntot < 200)

        crystals = client.get_crystals(query, flavor="ase")
        for crystal in crystals:
            self.assertIsNotNone(crystal)

        # now try getting the crystal from the phase_id(s)
        phase_ids = {_[0] for _ in client.get_data(query, fields={"S": ["phase_id"]})}
        crystals_from_phase_ids = client.get_crystals(
            query, phases=phase_ids, flavor="ase"
        )

        self.assertEqual(len(crystals), len(crystals_from_phase_ids))

    def test_retrieval_of_phases(self):
        """
        Look for intersection of query_a and query_b
        in two ways:
        maxnphases = changed and maxnphases = default
        """
        query_a = {"elements": "O", "classes": "binary", "props": "band gap"}
        query_b = {
            "elements": "O",
            "classes": "binary",
            "props": "isothermal bulk modulus",
        }

        client_one = MPDSDataRetrieval()
        client_one.maxnphases = 50

        answer_one = client_one.get_dataframe(
            query_a,
            fields={
                "P": ["sample.material.phase_id", "sample.material.chemical_formula"]
            },
            columns=["Phid", "Object"],
        )
        if not (isinstance(answer_one, pl.DataFrame)):
            print(type(answer_one))
            raise ValueError("answer_one is not a Polars DataFrame", type(answer_one))

        answer_one = answer_one.filter(pl.col("Phid").is_not_null())
        answer_one = answer_one.with_columns(pl.col("Phid").cast(pl.Int32))
        phases_one = answer_one["Phid"].to_list()

        self.assertTrue(len(phases_one) > client_one.maxnphases)

        result_one = client_one.get_dataframe(
            query_b,
            fields={
                "P": ["sample.material.phase_id", "sample.material.chemical_formula"]
            },
            columns=["Phid", "Object"],
            phases=phases_one,
        )

        client_two = MPDSDataRetrieval()
        self.assertEqual(client_two.maxnphases, MPDSDataRetrieval.maxnphases)

        answer_two = client_two.get_dataframe(
            query_a,
            fields={
                "P": ["sample.material.phase_id", "sample.material.chemical_formula"]
            },
            columns=["Phid", "Object"],
        )
        if not (isinstance(answer_one, pl.DataFrame)):
            print(type(answer_two))
            raise ValueError(
                "answer_one is not a Polars DataFrame, is", type(answer_two)
            )

        answer_two = answer_two.filter(pl.col("Phid").is_not_null())
        phases_two = answer_two["Phid"].cast(pl.Int32).to_list()

        self.assertTrue(len(phases_two) < client_two.maxnphases)

        result_two = client_two.get_dataframe(
            query_b,
            fields={
                "P": ["sample.material.phase_id", "sample.material.chemical_formula"]
            },
            columns=["Phid", "Object"],
            phases=phases_two,
        )

        self.assertEqual(len(result_one), len(result_two))

        # check equality of result_one and result_two
        merge = pl.concat([result_one, result_two])
        merge = merge.with_columns(pl.Series("index", range(len(merge))))
        merge_gpby = merge.group_by(list(merge.columns), maintain_order=True).agg(
            pl.len()
        )
        idx = [x[0] for x in merge_gpby.iter_rows() if x[-1] == 1]

        self.assertTrue(merge.filter(pl.col("index").is_in(idx)).is_empty())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
