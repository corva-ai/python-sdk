import pathlib
from importlib import machinery

import setuptools

ROOT = pathlib.Path(__file__).parent
LONG_DESCRIPTION = (
    f'{(ROOT / "README.md").read_text()}'
    f'\n\n'
    f'{(ROOT / "CHANGELOG.md").read_text()}'
)

VERSION = str(
    machinery.SourceFileLoader('version', 'src/version.py').load_module().VERSION
)

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.8',
    'Topic :: Software Development :: Libraries',
]

setuptools.setup(
    name='corva-sdk',
    author='Jordan Ambra',
    author_email="jordan.ambra@corva.ai",
    url='https://github.com/corva-ai/python-sdk',
    version=VERSION,
    classifiers=CLASSIFIERS,
    description='SDK for building Corva DevCenter Python apps.',
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    keywords='corva, sdk',
    py_modules=[file.stem for file in pathlib.Path('src').glob('*.py')],
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "fakeredis[lua] >=2.26.2, <2.30.0",
        "pydantic >=1.8.2, <2.0.0",
        "redis >=5.2.1, <6.0.0",
        "requests >=2.32.3, <3.0.0",
        "urllib3 <2",  # lambda doesnt support version 2 yet
    ],
    python_requires='>=3.8, <4.0',
    license='The Unlicense',
    entry_points={"pytest11": ["corva = plugin"]},
)
