#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='aribackup',
    version='1.0',
    description='Automates VM backups with LVM snapshots',
    url='https://github.com/AmericanResearchInstitute/ari-backup',
    packages=['aribackup',],
    maintainer='Michael Hrivnak',
    maintainer_email='mhrivnak@tireswingsoftware.com',
    license='BSD',
    classifiers = ['Development Status :: 5 - Production/Stable',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: System :: Archiving :: Backup'
    ],
)
