# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name='kmsorter',
    version='0.1',
    author='Martin Magr',
    author_email='martin.magr@gmail.com',
    packages=find_packages(),
    install_requires=[
        'click',
    ],
    entry_points={
        'console_scripts': [
            'kmsorter = kmsorter.cmd:main'
        ]
    }
)
