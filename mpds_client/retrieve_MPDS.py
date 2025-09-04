import logging
import math
import os
import shutil
import sys
import tempfile
import time
import warnings
from pathlib import Path
from urllib.parse import urlencode

import httplib2
import jmespath
import polars as pl
import py7zr
import requests
import ujson as json
from errors import APIError
from numpy import array_split

use_pmg, use_ase = False, False

try:
    from pymatgen.core.lattice import Lattice
    from pymatgen.core.structure import Structure

    use_pmg = True
except ImportError:
    pass

try:
    from ase import Atom
    from ase.spacegroup import crystal

    use_ase = True
except ImportError:
    pass


if not use_pmg and not use_ase:
    warnings.warn("Crystal structure treatment unavailable")

__author__ = "Evgeny Blokhin <eb@tilde.pro>"
__copyright__ = "Copyright (c) 2020, Evgeny Blokhin, Tilde Materials Informatics"
__license__ = "MIT"


class MPDSDataTypes(object):
    PEER_REVIEWED = 1
    MACHINE_LEARNING = 2
    AB_INITIO = 4
    ALL = 7


class MPDSDataRetrieval(object):
    """
    An example Python implementation
    of the API consumer for the MPDS platform,
    see https://developer.mpds.io

    Usage:
    $>export MPDS_KEY=...

    client = MPDSDataRetrieval()

    dataframe = client.get_dataframe({"formula":"SrTiO3", "props":"phonons"})

    *or*
    jsonobj = client.get_data(
        {"formula":"SrTiO3", "sgs": 99, "props":"atomic properties"},
        fields={
            'S':["entry", "cell_abc", "sg_n", "basis_noneq", "els_noneq"]
        }
    )

    *or*
    jsonobj = client.get_data({"formula":"SrTiO3"}, fields={})
    """

    default_fields = {
        "S": [
            "phase_id",
            "chemical_formula",
            "sg_n",
            "entry",
            lambda: "crystal structure",
            lambda: "angstrom",
        ],
        "P": [
            "sample.material.phase_id",
            "sample.material.chemical_formula",
            "sample.material.condition[0].scalar[0].value",
            "sample.material.entry",
            "sample.measurement[0].property.name",
            "sample.measurement[0].property.units",
            "sample.measurement[0].property.scalar",
        ],
        "C": [
            lambda: None,
            "title",
            lambda: None,
            "entry",
            lambda: "phase diagram",
            "naxes",
            "arity",
        ],
    }
    default_titles = ["Phase", "Formula", "SG", "Entry", "Property", "Units", "Value"]

    endpoint = "https://api.mpds.io/v0/download/facet"

    pagesize = 1000
    maxnpages = (
        120  # one hit may reach 50kB in RAM, consider pagesize*maxnpages*50kB free RAM
    )
    maxnphases = 1500  # more phases require additional requests
    chillouttime = 2  # please, do not use values < 2, because the server may burn out
    verbose = True
    debug = False

    def __init__(
        self, api_key=None, endpoint=None, dtype=None, verbose=None, debug=None
    ):
        """
        MPDS API consumer constructor

        Args:
            api_key: (str) The MPDS API key, or None if the MPDS_KEY envvar is set
            endpoint: (str) MPDS API gateway URL

        Returns: None
        """
        self.api_key = api_key if api_key else os.environ["MPDS_KEY"]

        self.network = httplib2.Http()

        self.endpoint = endpoint or self.endpoint
        self.dtype = dtype or MPDSDataTypes.PEER_REVIEWED
        self.verbose = verbose if verbose is not None else self.verbose
        self.debug = debug or self.debug

    def _request(self, query, phases=None, page=0, pagesize=None):
        phases = ",".join([str(int(x)) for x in phases]) if phases else ""

        uri = (
            self.endpoint
            + "?"
            + urlencode(
                {
                    "q": json.dumps(query),
                    "phases": phases,
                    "page": page,
                    "pagesize": pagesize or self.pagesize,
                    "dtype": self.dtype,
                }
            )
        )

        if self.debug:
            print('curl -XGET -HKey:%s "%s"' % (self.api_key, uri))

        response, content = self.network.request(
            uri=uri, method="GET", headers={"Key": self.api_key}
        )

        if response.status != 200:
            return {"error": content, "code": response.status}

        try:
            content = json.loads(content)
        except:
            return {"error": "Unreadable data obtained"}

        if content.get("error"):
            return {"error": content["error"]}

        if not content["out"]:
            return {"error": "No hits", "code": 204}

        return content

    def _massage(self, array, fields):
        if not fields:
            return array

        output = []

        for item in array:
            filtered = []

            for object_type in ["S", "P", "C"]:
                if item["object_type"] == object_type:
                    for expr in fields.get(object_type, []):
                        if isinstance(expr, jmespath.parser.ParsedResult):
                            filtered.append(expr.search(item))
                        else:
                            filtered.append(expr)
                    break
            else:
                raise APIError("API error: unknown entry type")

            output.append(filtered)

        return output

    def count_data(self, search, phases=None, **kwargs):
        """
        Calculate the number of entries matching the keyword(s) specified

        Args:
            search: (dict) Search query like {"categ_A": "val_A", "categ_B": "val_B"},
                documented at https://developer.mpds.io/#Categories
            phases: (list) Phase IDs, according to the MPDS distinct phases concept
            kwargs: just a mockup

        Returns:
            count (int)
        """
        result = self._request(search, phases=phases, pagesize=10)

        if result["error"]:
            raise APIError(result["error"], result.get("code", 0))

        if result["npages"] > self.maxnpages:
            warnings.warn(
                "\r\nDataset is too big, you may risk to change maxnpages from %s to %s"
                % (self.maxnpages, int(math.ceil(result["count"] / self.pagesize)))
            )

        return result["count"]

    def get_data(self, search, phases=None, fields=default_fields):
        """
        Retrieve data in JSON.
        JSON is expected to be valid against the schema
        at https://developer.mpds.io/mpds.schema.json

        Args:
            search: (dict) Search query like {"categ_A": "val_A", "categ_B": "val_B"},
                documented at https://developer.mpds.io/#Categories
            phases: (list) Phase IDs, according to the MPDS distinct phases concept
            fields: (dict or None) Data of interest for C-, S-, and P-entries,
                e.g. for phase diagrams: {'C': ['naxes', 'arity', 'shapes']},
                documented at https://developer.mpds.io/#JSON-schemata
                (if None is given, all the fields will be present)

        Returns:
            List of dicts: C-, S-, and P-entries, the format is
            documented at https://developer.mpds.io/#JSON-schemata
        """
        output = []
        fields = (
            {
                key: [
                    jmespath.compile(item) if isinstance(item, str) else item()
                    for item in value
                ]
                for key, value in fields.items()
            }
            if fields
            else None
        )

        tot_count = 0

        phases = list(set(phases)) if phases else []

        if len(phases) > self.maxnphases:
            all_phases = array_split(
                phases, int(math.ceil(len(phases) / self.maxnphases))
            )
        else:
            all_phases = [phases]

        nsteps = len(all_phases)

        for step, current_phases in enumerate(all_phases, start=1):
            counter, hits_count = 0, 0

            while True:
                result = self._request(
                    search, phases=list(current_phases), page=counter
                )
                if result["error"]:
                    raise APIError(result["error"], result.get("code", 0))

                if result["npages"] > self.maxnpages:
                    raise APIError(
                        "Too many hits (%s > %s), please, be more specific"
                        % (result["count"], self.maxnpages * self.pagesize),
                        2,
                    )
                output.extend(self._massage(result["out"], fields))

                if hits_count and hits_count != result["count"]:
                    raise APIError(
                        "API error: hits count has been changed during the query"
                    )

                hits_count = result["count"]

                time.sleep(self.chillouttime)

                if counter == result["npages"] - 1:
                    break

                counter += 1

                if self.verbose:
                    sys.stdout.write(
                        "\r\t%d%% of step %s from %s"
                        % ((counter / result["npages"]) * 100, step, nsteps)
                    )
                    sys.stdout.flush()

            tot_count += hits_count

        if len(output) != tot_count:
            raise APIError("API error: collected and declared counts of hits differ")

        if self.verbose:
            sys.stdout.write(" Got %s hits\r\n" % tot_count)
            sys.stdout.flush()

        return output

    def get_dataframe(self, *args, **kwargs):
        """
        Retrieve data as a Polars dataframe.

        Args:
            search: (dict) Search query like {"categ_A": "val_A", "categ_B": "val_B"},
                documented at https://developer.mpds.io/#Categories
            phases: (list) Phase IDs, according to the MPDS distinct phases concept
            fields: (dict or None) Data of interest for C-, S-, and P-entries,
                e.g. for phase diagrams: {'C': ['naxes', 'arity', 'shapes']},
                documented at https://developer.mpds.io/#JSON-schemata
                (if None is given, all the fields will be present)
            columns: (list) Column names for Pandas dataframe

        Returns: (object) Polars dataframe object containing the results
        """
        columns = kwargs.get("columns")
        if columns:
            del kwargs["columns"]
        else:
            columns = self.default_titles

        data = self.get_data(*args, **kwargs)
        return pl.DataFrame(data, schema=columns)

    def get_crystals(self, search, phases=None, flavor="pmg", **kwargs):
        search["props"] = "atomic structure"

        crystals = []
        for crystal_struct in self.get_data(
            search,
            phases,
            fields={"S": ["cell_abc", "sg_n", "basis_noneq", "els_noneq"]},
            **kwargs,
        ):
            crobj = self.compile_crystal(crystal_struct, flavor)
            if crobj is not None:
                crystals.append(crobj)

        return crystals

    @staticmethod
    def compile_crystal(datarow, flavor="pmg"):
        """
        Helper method for representing the MPDS crystal structures in two flavors:
        either as a Pymatgen Structure object, or as an ASE Atoms object.

        Attention #1. Disordered structures (e.g. fractional indices in the chemical formulae)
        are not supported by this method, and hence the occupancies are not retrieved.
        Currently it's up to the user to take care of that (see e.g.
        https://doi.org/10.1186/s13321-016-0129-3 etc.).

        Attention #2. Pymatgen and ASE flavors are generally not compatible, e.g.
        primitive vs. crystallographic cell is defaulted,
        atoms wrapped or non-wrapped into the unit cell, etc.

        Note, that the crystal structures are not retrieved by default,
        so for them one needs to specify the following fields:
            - cell_abc
            - sg_n
            - basis_noneq
            - els_noneq
        e.g. like this: {'S':['cell_abc', 'sg_n', 'basis_noneq', 'els_noneq']}

        Args:
            datarow: (list) Required data to construct crystal structure:
                [cell_abc, sg_n, basis_noneq, els_noneq]
            flavor: (str) Either "pmg", or "ase"

        Returns:
            - if flavor is pmg, Pymatgen Structure object
            - if flavor is ase, ASE Atoms object
        """
        if not datarow or not datarow[-1]:
            # this is either a P-entry with the cell data, which meets the search criterion,
            # or a 'low quality' structure with no basis (just unit cell parameters)
            return None

        if len(datarow) < 4:
            raise ValueError(
                "Must supply a data row that ends with the entries "
                "'cell_abc', 'sg_n', 'basis_noneq', 'els_noneq'"
            )

        cell_abc, sg_n, basis_noneq, els_noneq = (
            datarow[-4],
            int(datarow[-3]),
            datarow[-2],
            datarow[-1],
        )

        if flavor == "pmg" and use_pmg:
            return Structure.from_spacegroup(
                sg_n, Lattice.from_parameters(*cell_abc), els_noneq, basis_noneq
            )

        elif flavor == "ase" and use_ase:
            atom_data = []

            for num, i in enumerate(basis_noneq):
                atom_data.append(Atom(els_noneq[num], tuple(i)))

            return crystal(
                atom_data,
                spacegroup=sg_n,
                cellpar=cell_abc,
                primitive_cell=True,
                onduplicates="replace",
            )

        else:
            raise APIError("Crystal structure treatment unavailable")
        
    @staticmethod
    def extract_7z(archive_path: Path, target_dir: Path):
        """Unpack 7z archive to target dir"""
        try:
            with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                archive.extractall(target_dir)
            return True
        except Exception as e:
            print(f"Error during unpack {archive_path}: {e}")
            return False

    def download_ab_initio_logs(
        self,
        search: dict,
        save_dir: Path,
        keep_archives: bool = False,
        timeout: int = 30,
    ):
        """
        Download ab initio simulation logs (CRYSTAL .out and Fleur .xml) for materials matching the search criteria.

        Args:
            search (dict): Search query like {"props": "electrical conductivity"}
            save_dir (str|Path): Directory to save downloaded logs
            keep_archives (bool): Whether to keep downloaded archive files
            timeout (int): Timeout for download requests in seconds

        Returns:
            list: Paths to downloaded log files
        """
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)
        archive_dir = save_dir / "temp_archives"
        archive_dir.mkdir(exist_ok=True)

        # aetup logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(save_dir / "ab_initio_downloader.log"),
            ],
        )
        logger = logging.getLogger("MPDSDataRetrieval")

        # get URLs
        fields = {
            "P": [
                "sample.material.entry",
                "sample.material.phase_id",
                "sample.measurement[0].raw_data",
            ]
        }
        data = self.get_data(search, fields=fields)

        if not data:
            logger.warning("No ab initio data found matching the search criteria")
            return []

        saved_files = []
        for item in data:
            material_id = item[0]
            phase_id = item[1]
            archive_url = item[2]

            if not archive_url:
                logger.warning(f"No archive URL for material {material_id}")
                continue

            logger.info(f"Processing material {material_id}")

            try:
                # download archive
                response = requests.get(archive_url, timeout=timeout)
                response.raise_for_status()

                # save archive
                archive_path = archive_dir / f"material_{material_id}.7z"
                with open(archive_path, "wb") as f:
                    f.write(response.content)
                logger.info(f"Saved archive: {archive_path}")

                # unpack
                material_files = self._extract_logs(
                    archive_path, material_id, phase_id, save_dir, self.extract_7z, logger
                )
                saved_files.extend(material_files)
                logger.info(
                    f"Extracted {len(material_files)} logs for material {material_id}"
                )

                # delete archive if not keeping archives
                if not keep_archives:
                    archive_path.unlink()

            except Exception as e:
                logger.error(f"Error processing material {material_id}: {str(e)}")

        # delete temp archive dir if not keeping archives
        if not keep_archives:
            shutil.rmtree(archive_dir, ignore_errors=True)

        logger.info(f"Downloaded {len(saved_files)} log files in total")
        return saved_files

    def _extract_logs(
        self, archive_path: Path, material_id: str, phase_id: str, save_dir: Path, extract_func, logger
    ):
        """Extract engines logs"""
        material_dir = save_dir / f"material_{material_id}"
        material_dir.mkdir(exist_ok=True)
        saved_files = []

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # unpack archive
            success = extract_func(archive_path, tmp_path)
            if not success:
                logger.error(f"Failed to extract archive: {archive_path.name}")
                return saved_files

            logger.debug(f"Extracted archive: {archive_path.name} to {tmp_path}")

            # try to find and save log files
            for file_path in tmp_path.rglob("*"):
                if not file_path.is_file():
                    continue

                # check if the file is a log file
                if (
                    file_path.suffix in (".out", ".xml")
                    or file_path.name == "SIGMA.DAT"
                    or "TRANSPORT" in file_path.parts
                ):

                    # create new name with phase info
                    new_name = f"phase_{phase_id}_{file_path.name}"
                    dest_path = material_dir / new_name

                    shutil.copy2(file_path, dest_path)
                    saved_files.append(dest_path)
                    logger.info(f"Saved log: {dest_path}")

        return saved_files


if __name__ == "__main__":
    client = MPDSDataRetrieval(dtype=MPDSDataTypes.AB_INITIO)
    downloaded_files = client.download_ab_initio_logs(
        search={"props": "electrical conductivity"},
        save_dir="./ab_initio_logs",
        keep_archives=False,
    )

    print(f"Downloaded {len(downloaded_files)} log files")
