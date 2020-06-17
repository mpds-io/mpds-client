
import sys

from .retrieve_MPDS import MPDSDataTypes, APIError, MPDSDataRetrieval
from .export_MPDS import MPDSExport


MIN_PY_VER = (3, 5)

assert sys.version_info >= MIN_PY_VER, "Python version must be >= {}".format(MIN_PY_VER)