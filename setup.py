import pathlib
from importlib import machinery

import setuptools

ROOT = pathlib.Path(__file__).parent
README = (ROOT / "README.md").read_text()

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
    description='SDK for interacting with Corva',
    long_description=README,
    long_description_content_type="text/markdown",
    keywords='corva, sdk',
    py_modules=[file.stem for file in pathlib.Path('src').glob('*.py')],
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "fakeredis ~=1.4.5",
        "pydantic ~=1.7.3",
        "redis ~=3.5.3",
        "requests ~=2.25.0",
        "requests-mock ~=1.8.0",
    ],
    extras_require={
        "dev": [
            "coverage ==5.3",
            "flake8 ==3.8.4",
            "freezegun ==1.0.0",
            "pytest ==6.1.2",
            "pytest-mock ==3.3.1",
        ]
    },
    python_requires='~=3.8',
    license='The Unlicense',
    entry_points={"pytest11": ["corva = plugin"]},
)
