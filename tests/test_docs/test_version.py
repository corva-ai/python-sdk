import pathlib
import re

import version


def test_doc_and_lib_versions_match():
    doc_text = pathlib.Path('docs/index.adoc').read_text()
    # searches text like: Documentation for version *v0.0.18*
    doc_match = re.compile(r'Documentation for version \*v.*\..*\..*').search(doc_text)
    doc_version = doc_match.group().strip('.*').split('v')[-1]

    assert version.VERSION == doc_version
