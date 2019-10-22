======
jpxlab
======


.. image:: https://img.shields.io/pypi/v/jpxlab.svg
        :target: https://pypi.python.org/pypi/jpxlab

.. image:: https://img.shields.io/travis/exilis/jpxlab.svg
        :target: https://travis-ci.org/exilis/jpxlab

.. image:: https://readthedocs.org/projects/jpxlab/badge/?version=latest
        :target: https://jpxlab.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/exilis/jpxlab/shield.svg
     :target: https://pyup.io/repos/github/exilis/jpxlab/
     :alt: Updates



The data analysis sandbox for JPX


* Free software: MIT license


Features
--------

* Historical data conversion
* [TODO] Example notebook to analyze the data

Usage: download
--------

* Prerequisites: You have to contact with JPX's account manager and get FTP account

.. code-block::

  $ cd tools/fetcher
  $ vim fetch.sh
  
  # edit `FTP_USER` and `FTP_PASS`
  
  $ ./build.sh
  $ ./fetch.sh 20191008
  
* The file is downloaded into `<repos root>/downloads`
* You can also specify wiledcard to dowonload multiple files in batch (e.g. `./fetch.sh '201909??'`)
* It fetches from under `/archives/` so most recent files are out of scope


Usage: convert from raw zip files to h5
--------

.. code-block::

    $ python cli.py convert --help
    Usage: cli.py convert [OPTIONS] [FILES]...

      convert raw zip files to h5

    Options:
      --help  Show this message and exit.
      
Usage: resample h5 files into aggregated dataframe
--------

.. code-block::

    $ python cli.py resample --help
    Usage: cli.py resample [OPTIONS] [FILES]...

      resample the h5 file into aggregated dataframe

    Options:
      -f, --freq TEXT  frequency of resampling (e.g. '1H' for hourly aggregation)
      --help           Show this message and exit.


Usage: launch the jupyter notebook (locally)
--------

  $ make notebook

Usage: launch the jupyter notebook (in docker)
--------

  $ make notebook_docker
  
Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
