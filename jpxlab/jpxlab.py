# -*- coding: utf-8 -*-

from zipfile import ZipFile, BadZipFile
import os
import paramiko
import struct
import tables


def get_sftp_session(host, port, username, password):
    """Get a SFTP session

    Args:
        host(str)       : host name
        port:(int)      : port number
        username(str)   : user name

    Returns:
        A SFTP session object
    """
    transport = paramiko.Transport((host, port))
    transport.connect(None, username, password)
    return paramiko.SFTPClient.from_transport(transport)


def _load_chunk(z):
    """Load the entire chunk and parse the header

    Args:
        z (ZipExtFile)  : zip input stream

    Returns:
        (
            payload,    # payload of the chunk
            exchange,   # exchange code
            session,    # session code
            category,   # category code
            security    # security code
        )
    """

    fmt_header = '1c6s11s3s1c2s4s12s1c'
    size_header = struct.calcsize(fmt_header)

    buf = z.read(size_header)

    # Extract the header
    header = struct.unpack(fmt_header, buf)
    (
        _, chunk_size, _, _, exchange, session, category, security, _
    ) = header
    chunk_size = int(chunk_size)

    # Read Block
    payload = z.read(chunk_size - size_header)
    exchange = exchange.strip().decode("utf-8")
    security = security.strip().decode("utf-8")

    return (
        payload,
        exchange,
        session,
        category,
        security
    )


def _parse_chunk(payload):
    """Parse the chunk and yields for each tag

    Args:
        payload(str) : payload of the chunk
    """

    # Define block formats
    fmt_4p = "2s2s1b14s1c6s1c1c1c14s1c6s1c1c1c14s1c6s1c1c14s1c12s1c2s1c"
    fmt_vl = "2s2s1c1c14s6s1c"

    # Loop through the Tags (divided by b'\x13')
    for c in payload.split(b'\x13'):

        tag_id = c[0:2]

        # Check if it is a Price block
        if tag_id == b'4P':
            val_4p = struct.unpack(fmt_4p, c)
            (
                _, _,
                o_flag, o_price, o_sign, o_timestamp, o_changed,
                h_stop, h_flag, h_price, h_sign, h_timestamp, h_changed,
                l_stop, l_flag, l_price, l_sign, l_timestamp, l_changed,
                cur_flag, cur_price, cur_sign, cur_timestamp, cur_changed,
                _, _) = val_4p

            yield (
                tag_id,
                (
                    cur_timestamp.decode("utf-8"),
                    int(o_price),
                    int(h_price),
                    int(l_price),
                    int(cur_price),
                )
            )

        # Check if it is a Volumne block
        elif tag_id == b'VL':
            val_vl = struct.unpack(fmt_vl, c)
            _, _, _, flag, vol, timestamp, _ = val_vl

            yield (
                tag_id,
                (
                    timestamp.decode("utf-8"),
                    int(vol),
                )
            )

        else:
            # we don't need other chunks
            pass


def _dump_to_h5(z, store):
    """Convert and dump to h5

    Args:
        z (ZipExtFile)             : zip input stream
        store (pytable out stream) : pytable out
    """

    out_price_idx = dict()
    out_price = dict()
    out_volume_idx = dict()
    out_volume = dict()

    while not z._eof:

        try:
            chunk = _load_chunk(z)
            if chunk is None:
                continue
        except BadZipFile:
            break

        payload, exchange, session, category, security = chunk
        key = (exchange, security)

        for typ, row in _parse_chunk(payload):

            if typ == b'4P':

                if key not in out_price:
                    out_price_idx[key] = store.create_earray(
                        '/{}/price_idx'.format(exchange),
                        security, obj=row[:1], createparents=True)
                    out_price[key] = store.create_earray(
                        '/{}/price'.format(exchange),
                        security, obj=row[1:], createparents=True)
                else:
                    out_price_idx[key].append(row[:1])
                    out_price[key].append(row[1:])

            elif typ == b'VL':

                if key not in out_volume:
                    out_volume_idx[key] = store.create_earray(
                        '/{}/volume_idx'.format(exchange),
                        security, obj=row[:1], createparents=True)
                    out_volume[key] = store.create_earray(
                        '/{}/volume'.format(exchange),
                        security, obj=row[1:], createparents=True)
                else:
                    out_volume_idx[key].append(row[:1])
                    out_volume[key].append(row[1:])


def _get_out_filename(src, out_dir):
    return os.path.join(
        out_dir,
        os.path.basename(src).replace(".zip", ".h5"),
    )


def fetch_and_convert(sftp, src, out_dir):

    assert src.endswith(".zip")

    # Open the SFTP ZIP File
    with sftp.open(src, "r") as f:
        zip_file = ZipFile(f)
        # Open the first compressed file
        # (Only expecting one file inside the ZIP)
        with zip_file.open(zip_file.namelist()[0]) as z:
            out_filename = _get_out_filename(src, out_dir)
            with tables.open_file(out_filename, mode='w') as store:
                _dump_to_h5(z, store)
