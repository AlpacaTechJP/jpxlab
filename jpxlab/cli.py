# -*- coding: utf-8 -*-

"""Console script for jpxlab."""
from joblib import Parallel, delayed
import click
import jpxlab
import sys


@click.group()
def cmd():
    pass


@cmd.command()
@click.option("-f", "--freq", "freq", type=str, help="frequency of resampling")
@click.argument("files", nargs=-1, type=click.Path())
def resample(freq, files):
    """resample the h5 file into aggregated dataframe"""

    Parallel(n_jobs=-1)(
        delayed(jpxlab.resample)(f, f.replace(".h5", "_{}.h5".format(freq)), freq)
        for f in files
    )

    return 0


@cmd.command()
@click.argument("files", nargs=-1, type=click.Path())
def convert(files):
    """convert raw zip files to h5"""

    Parallel(n_jobs=-1)(delayed(jpxlab.fetch_and_convert)(f) for f in files)

    return 0


if __name__ == "__main__":
    sys.exit(cmd())
