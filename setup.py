import pathlib
import setuptools

root = pathlib.Path(__file__).parent
readme = (root / "README.md").read_text()

version_path = root / "corva" / "version.py"
with open(version_path) as version_file:
    version = ""
    # Execute the code in version.py.
    exec(compile(version_file.read(), version_path, 'exec'))


classifiers = [
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
    version=version,
    classifiers=classifiers,
    description='SDK for interacting with Corva',
    long_description=readme,
    long_description_content_type="text/markdown",
    keywords='corva, sdk',
    packages=setuptools.find_packages(exclude=[".*", "data", "test"]),
    install_requires=[
        "pydantic >= 1.7.2",
        "redis >= 3.5.3",
        "requests >= 2.25.0",
        "urllib3 >= 1.26.2"
    ],
    license='The Unlicense',
)
