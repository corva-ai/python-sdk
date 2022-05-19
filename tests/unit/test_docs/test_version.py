import pathlib
import re

import pytest

import version


def test_doc_and_lib_versions_match():
    doc_text = pathlib.Path('docs/index.adoc').read_text()
    # searches text like: Documentation for version *v0.0.18*
    doc_match = re.compile(r'Documentation for version \*v.*\..*\..*').search(doc_text)

    if doc_match is None:
        pytest.fail('Could not find documentation version.')

    doc_version = doc_match.group().strip('.*').split('v')[-1]

    assert version.VERSION == doc_version
