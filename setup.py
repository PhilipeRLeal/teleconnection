#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages


from version_file_seeker import find_version

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()
	


Version = find_version('', '__init__.py')


with open('requirements_dev.txt') as requirements_file:
    read_lines = requirements_file.readlines()
    requirements = [line.rstrip('\n') for line in read_lines]
	

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(


    author="Philipe Riskalla Leal",
    author_email='leal.philipe@gmail.com',
    classifiers=[
        'Topic :: Education',                                        # this line follows the options given by: [1]
        "Topic :: Scientific/Engineering",    # this line follows the options given by: [1]		
        'Intended Audience :: Education',
		"Intended Audience :: Science/Research",
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
		'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="""This library operates Space-Time Match-Up operations over Netcdf-Xarray datasets and Geopandas-GeoDataFrames. \
				 It is a mandatoryr step for areas of study as geography, epidemiology, sociology, remote sensing, ecology, etc.""",
    
    install_requires=requirements,
    license="MIT license",
    
    include_package_data=True,
	
	python_requires='>=3.7',  # Your supported Python ranges
	
    keywords='teleconnection xarray geopandas space-time reduction',
    name='teleconnection',
	
	packages=find_packages(include='/teleconnection/*'),
	package_dir = {'teleconnection': 'teleconnection'},
    
    setup_requires=setup_requirements,
    #test_suite='nose.collector',
    #tests_require=['nose'],
    url='https://github.com/PhilipeRLeal/teleconnection',
    version=Version,
    zip_safe=False,
)




# [1]: https://pypi.org/classifiers/