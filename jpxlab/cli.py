# -*- coding: utf-8 -*-

"""Console script for jpxlab."""
import click
import jpxlab
import os
import sys


@click.command()
@click.option(
    '-h', '--host', default="ftp.tmi.tse.or.jp",
    help='host name of the sftp server')
@click.option(
    '-p', '--port', default=21, type=int,
    help='port number of the sftp server')
@click.option(
    '-u', '--user',
    help='user id to login to the sftp server')
@click.option('-s', '--src', help='source path')
@click.option('-o', '--out-dir', default="./", help='output directory')
def main(host, port, user, src, out_dir):
    """Console script for jpxlab."""

    password = os.environ.get("JPXLAB_SFTP_PASS", None)
    if password is None:
        password = click.prompt(
            'Please enter a password for the sftp server', type=str)

    sftp = jpxlab.get_sftp_session(host, port, user, password)

    jpxlab.fetch_and_convert(sftp, src, out_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
