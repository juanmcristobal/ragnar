======
Ragnar
======


.. image:: https://img.shields.io/pypi/v/ragnar.svg
        :target: https://pypi.python.org/pypi/ragnar

.. image:: https://img.shields.io/travis/juanmcristobal/ragnar.svg
        :target: https://travis-ci.org/juanmcristobal/ragnar

.. image:: https://readthedocs.org/projects/ragnar/badge/?version=latest
        :target: https://ragnar.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://coveralls.io/repos/github/juanmcristobal/ragnar/badge.svg?branch=master
	:target: https://coveralls.io/github/juanmcristobal/ragnar?branch=master



Ragnar is a lightweight Extract-Transform-Load (ETL) framework for Python 3.5+.


* Free software: MIT license
* Documentation: https://ragnar.readthedocs.io.


Features
--------

* Keeps a functional programming philosophy.
* Code reuse instead of "re-inventing the wheel" in each script.
* Customizable for your organization's particular tasks.

Example
-------

A pipeline that applies capital letters to the list and then filters through the one starting with "B":

.. code:: python

    >>> from ragnar.stream import Stream
    >>> st = Stream(["apple", "banana", "cherry"])
    >>> st.do(lambda x: x.upper())
    <ragnar.stream.Stream object at 0x7fbe8e3509d0>
    >>> st.filter(lambda x:x.startswith("B"))
    <ragnar.stream.Stream object at 0x7fbe8e3509d0>
    >>> for row in st:
    ...     print(row)
    BANANA


