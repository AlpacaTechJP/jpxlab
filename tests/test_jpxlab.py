#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `jpxlab` package."""

import pytest
import io
from click.testing import CliRunner

from jpxlab import jpxlab
from jpxlab import cli


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
    stream = io.BytesIO(b'\x11  1295003001320721001010111       1234 '
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
        b'\x13VW   0      20380000+090000  0      20380000+090000 \x11')
    
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

