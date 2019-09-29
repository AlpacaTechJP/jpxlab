#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `jpxlab` package."""

import pytest
import io
import os
import tables

from click.testing import CliRunner

from jpxlab import jpxlab
from jpxlab import cli

STREAM = (b'\x11  1295003001320721001010111       1234 '
    b'\x12NO    3351'
    b'\x13ST  120  0090000063886    '
    b'\x134P  4      20380000+0900001 4      20380000+0900001 4      20380000+09000014      20380000+0900000638861   '
    b'\x13Q1  14      20390000+09000006388610           900+14      20380000+09000006388610          9300+'
    b'\x13Q2  14      20400000+09000006388610          2100+14      20370000+09000006388610          4800+'
    b'\x13Q3  14      20410000+09000006388610          2300+14      20360000+09000006388610         11800+'
    b'\x13Q4  14      20420000+09000006388610          2600+14      20350000+09000006388610          9200+'
    b'\x13Q5  14      20430000+09000006388610          2500+14      20340000+09000006388610          2700+'
    b'\x13Q6  14      20440000+09000006388610          2100+14      20330000+09000006388610          3500+'
    b'\x13Q7  14      20450000+09000006388610          2100+14      20320000+09000006388610          6500+'
    b'\x13Q8  14      20460000+09000006388610          2300+14      20310000+09000006388610         10300+'
    b'\x13Q9  14      20470000+09000006388610          1600+14      20300000+09000006388610         23500+'
    b'\x13QM  1                            1                            '
    b'\x13QO  10900000638860        701200+10900000638860        823600+'
    b'\x13VL   0        159700090000 '
    b'\x13VA   0     325468600090000 '
    b'\x13VW   0      20380000+090000  0      20380000+090000 \x11'
    b'\x11   296002001178301008010124       9876 '
    b'\x12NO     294'
    b'\x13ST  120  0090000070955    '
    b'\x134P  4      19940000+0900001 4      19940000+0900001 4      19940000+09000014      19940000+0900000709551   '
    b'\x13VL   0          1400090000 '
    b'\x13VA   0       2791600090000 '
    b'\x13VW   0      19940000+090000  0      19940000+090000 \x11'
    b'\x11  1329010001872331008010124       9876 '
    b'\x12NO    1392'
    b'\x13ST   20  0090000105672    '
    b'\x134P  4      21830000+090000  4      21860000+090003  4      21790000+090001 4      21830000+0900065839991   '
    b'\x13Q1  14      21850000+09000658399910           200+14      21820000+09000658399920           200+'
    b'\x13Q2  14      21860000+09000658399910           900+14      21810000+09000658399910           900+'
    b'\x13Q3  14      21870000+09000658399910          1000+14      21800000+09000658399910           300+'
    b'\x13Q4  14      21880000+09000658399910          1500+14      21790000+09000658399910          1900+'
    b'\x13Q5  14      21890000+09000658399910           900+14      21780000+09000658399910           700+'
    b'\x13Q6  14      21900000+09000658399910          1800+14      21770000+09000658399910          1400+'
    b'\x13Q7  14      21910000+09000658399910          2200+14      21760000+09000658399910           600+'
    b'\x13Q8  14      21920000+09000658399910          5500+14      21750000+09000658399910           700+'
    b'\x13Q9  14      21930000+09000658399910          1300+14      21740000+09000658399910          1900+'
    b'\x13QA  14      21940000+09000658399910          1000+14      21730000+09000658399910          4200+'
    b'\x13QO  10900065839990        141700+10900065839990         92000+'
    b'\x13VL   0         53000090006 '
    b'\x13VA   0     115698400090006 '
    b'\x13VW   0      21829887+090006  0      21829887+090006 \x11')

@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string


def test_command_line_interface():
    """Test the CLI."""
    # runner = CliRunner()
    # result = runner.invoke(cli.main)
    # assert result.exit_code == 0
    # assert 'jpxlab.cli.main' in result.output
    # help_result = runner.invoke(cli.main, ['--help'])
    # assert help_result.exit_code == 0
    # assert '--help  Show this message and exit.' in help_result.output

def test_get_security_code():
    """
     Test the function _load_chunk and _parse_chunk by creating a sample, well structured stream
    """
    result = jpxlab._get_security_code("1", "1234")
    assert result == "t1234"

def test_stream_loading_and_parsing():
    """
     Test the function _get_security_code
    """
    stream = io.BytesIO(STREAM)
    
    # Read First Block
    payload, exchange, session, category, security, chunk_size = jpxlab._load_chunk(stream)
    
    assert exchange == '1'
    assert session == b'01'
    assert category == b'0111'
    assert security == '1234'
    assert chunk_size == 1295

    generator = jpxlab._parse_chunk(payload)
    
    typ, row = next(generator)
    assert typ == b'4P'

    typ, row = next(generator)
    assert typ == b'VL'

    # Read Second Block
    payload, exchange, session, category, security, chunk_size = jpxlab._load_chunk(stream)
    
    assert exchange == '8'
    assert session == b'01'
    assert category == b'0124'
    assert security == '9876'
    assert chunk_size == 296

    generator = jpxlab._parse_chunk(payload)
    
    typ, row = next(generator)
    assert typ == b'4P'

    typ, row = next(generator)
    assert typ == b'VL'

def test_hdf5_file_creation(tmpdir):
    """
     Test the HDF5 Creation
    """
    stream = io.BytesIO(STREAM)

    out_dir = tmpdir.mkdir("HDF5")
    src = "test.zip"
    out_filename = jpxlab._get_out_filename(src, out_dir, "_raw")

    assert out_filename[-17:] == "/HDF5/test_raw.h5"

    jpxlab._convert_and_store(stream, out_filename, 2920)

    assert os.path.exists(out_filename)

    with tables.open_file(out_filename, mode="r") as reader:
        
        # Check that the First Group is for prices
        price_group = reader.list_nodes('/')[0]        
        assert price_group._v_name == "price"

        prices = price_group._f_iter_nodes()

        # Check that the first block is for Sapporo Stock 9876 with 2 rows
        price1 = next(prices)
        assert price1._v_name == 's9876'
        assert price1.nrows == 2

        # Check that the second block is for Tokyo Stock 1234 with 1 row
        price2 = next(prices)
        assert price2._v_name == 't1234'
        assert price2.nrows == 1

    os.remove(out_filename)
    os.rmdir(out_dir)