
from setuptools import setup

install_requires = [
    'httplib2',
    'ujson',
    'numpy',
    'polars',
    'jmespath',
    'jsonschema',
    'ase' # also pymatgen is supported
]

setup(
    name='mpds_client',
    version='0.30',
    author='Evgeny Blokhin',
    author_email='eb@tilde.pro',
    description='MPDS platform API client',
    long_description='This Python library facilitates using the MPDS platform API (see developer.mpds.io) in such aspects as pagination, error handling, validation, proper data extraction and more. We encourage our users to adopt this library for their needs.',
    url='https://github.com/mpds-io/mpds-client',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Chemistry',
        'Topic :: Scientific/Engineering :: Physics',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: 3.14',
        'Programming Language :: Python :: 3.15',
    ],
    keywords='materials informatics crystal structures phase diagrams physical properties PAULING FILE MPDS platform API',
    packages=['mpds_client'],
    install_requires=install_requires,
    python_requires='>=3.8'
)
