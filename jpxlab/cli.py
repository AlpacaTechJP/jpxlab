# -*- coding: utf-8 -*-

"""Console script for jpxlab."""
import click
import jpxlab
import os
import sys


@click.group()
def cmd():
    pass


@cmd.command()
@click.option(
    '-h', '--host', default="ftp.tmi.tse.or.jp",
    help='host name of the sftp server')
@click.option(
    '-p', '--port', default=21, type=int,
    help='port number of the sftp server')
@click.option(
    '-u', '--user',
    help='user id to login to the sftp server')
@click.option('-l', '--local', is_flag=True, help='Required if working with a local file')
@click.option('-s', '--src', help='source path')
@click.option('-o', '--out-dir', default="./", help='output directory')
def fetch(host, port, user, local, src, out_dir):
    """Console script for jpxlab."""
    
    if local:
        sftp = None
    else:
        password = os.environ.get("JPXLAB_SFTP_PASS", None)
        if password is None:
            password = click.prompt(
                'Please enter a password for the sftp server', type=str, hide_input=True)

        sftp = jpxlab.get_sftp_session(host, port, user, password)

    jpxlab.fetch_and_convert(sftp, src, out_dir)

    return 0


@cmd.command()
@click.option('-s', '--src', help='source path')
@click.option('-o', '--out', help='output path')
def resample(src, out):
    """Resample the file fetched into 1sec aggregated dataframe"""

    jpxlab.resample(src, out)

    return 0


@cmd.command()
@click.option(
    '-h', '--host', default="ftp.tmi.tse.or.jp",
    help='host name of the sftp server')
@click.option(
    '-p', '--port', default=21, type=int,
    help='port number of the sftp server')
@click.option(
    '-u', '--user',
    help='user id to login to the sftp server')
@click.option('-l', '--local', is_flag=True, help='Required if working with a local file')
@click.option('-s', '--src', help='source path')
@click.option('-o', '--out-dir', default="./", help='output directory')
def fetch_resample(host, port, user, local, src, out_dir):
    """Fetch and resample"""
    
    if local:
        sftp = None
    else:
        password = os.environ.get("JPXLAB_SFTP_PASS", None)
        if password is None:
            password = click.prompt(
                'Please enter a password for the sftp server', type=str, hide_input=True)

        sftp = jpxlab.get_sftp_session(host, port, user, password)

    out_filename = jpxlab.fetch_and_convert(sftp, src, out_dir)

    jpxlab.resample(out_filename, out_filename.replace("_raw.h5", ".h5"))

    return 0


if __name__ == "__main__":
    sys.exit(cmd())
