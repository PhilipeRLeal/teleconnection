
"""
    " ============================================================================
    " Netrw Directory Listing                                        (netrw v155)
    "   /home/repl/mortgage_forecasts/__pycache__
    "   Sorted by      name
    "   Sort sequence: [\/]$,\<core\%(\.\d\+\)\=\>,\.h$,\.c$,\.cpp$,\~\=\*$,*,\.o$,\.obj$,\.info$
    "   Quick Help: <F1>:help  -:go up dir  D:delete  R:rename  s:sort-by  x:special

"""


import os
import re
import codecs

here = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")
