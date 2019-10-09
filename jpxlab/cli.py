# -*- coding: utf-8 -*-

"""Console script for jpxlab."""
import click
import jpxlab
import sys


@click.group()
def cmd():
    pass


@cmd.command()
@click.option("-s", "--src", help="source path")
@click.option("-o", "--out", help="output path")
def resample(src, out):
    """resample the h5 file into 1sec aggregated dataframe"""

    jpxlab.resample(src, out)

    return 0


@cmd.command()
@click.option("-s", "--src", help="source path")
@click.option("-o", "--out-dir", default="./", help="output directory")
def convert(src, out_dir):
    """convert raw file to h5"""

    jpxlab.fetch_and_convert(None, src, out_dir)

    return 0


@cmd.command()
@click.option("-s", "--src", help="source path")
@click.option("-o", "--out-dir", default="./", help="output directory")
def convert_resample(src, out_dir):
    """convert and resample"""

    out_filename = jpxlab.fetch_and_convert(None, src, out_dir)

    jpxlab.resample(out_filename, out_filename.replace("_raw.h5", ".h5"))

    return 0


if __name__ == "__main__":
    sys.exit(cmd())
