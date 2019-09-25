# -*- coding: utf-8 -*-

from zipfile import ZipFile
import gzip
import numpy as np
import os
import pandas as pd
import paramiko
import struct
import tables
from io import BytesIO
from tqdm import tqdm


def _load_chunk(stream: BytesIO) -> tuple:
    """Load the entire chunk and parse the header in the FLEX stream

    The detailed explanation can be found in 5.3.2 of
    `01_Market Information System FLEX Connection Specification Common Items DS.15.10.pdf`.

    Args:
        stream (BytesIO)  : input stream

    Returns:
        (
            payload,    # payload of the chunk
            exchange,   # exchange code
            session,    # session code
            category,   # category code
            security    # security code
        )
    """

    fmt_header = "1c6s11s3s1c2s4s12s1c"
    size_header = struct.calcsize(fmt_header)

    buf = stream.read(size_header)
    if len(buf) < size_header:
        return None

    # Extract the header
    header = struct.unpack(fmt_header, buf)
    (_, chunk_size, _, _, exchange, session, category, security, _) = header
    chunk_size = int(chunk_size)

    # Read a block
    payload = stream.read(chunk_size - size_header).strip(b"\x11")
    exchange = exchange.strip().decode("utf-8")
    security = security.strip().decode("utf-8")

    return (payload, exchange, session, category, security, chunk_size)


def _parse_chunk(payload: bytes) -> tuple:
    """Parse the chunk and yields for each tag

    Extract `4P` and `VL` tags. Detailed explanations can be found in

    "04_Market Information System FLEX Connection Specification Index¥Statistics group DS.15.10.pdf"

    Args:
        payload(str) : payload of the chunk
    """

    # Define block formats
    fmt_4p = "2s2s1b14s1c6s1c1c1c14s1c6s1c1c1c14s1c6s1c1c14s1c12s1c2s1c"
    fmt_vl = "2s2s1c1c14s6s1c"

    # Loop through the Tags (divided by b'\x13')
    for c in payload.split(b"\x13"):

        tag_id = c[0:2]

        # Check if it is a Price block
        if tag_id == b"4P":
            val_4p = struct.unpack(fmt_4p, c)
            (
                _,
                _,
                o_flag,
                o_price,
                o_sign,
                o_timestamp,
                o_changed,
                h_stop,
                h_flag,
                h_price,
                h_sign,
                h_timestamp,
                h_changed,
                l_stop,
                l_flag,
                l_price,
                l_sign,
                l_timestamp,
                l_changed,
                cur_flag,
                cur_price,
                cur_sign,
                cur_timestamp,
                cur_changed,
                _,
                _,
            ) = val_4p

            if len(cur_timestamp.strip()) > 0:
                h, m, s, ms = list(struct.unpack("2s2s2s6s", cur_timestamp))

                yield (
                    tag_id,
                    (
                        (int(h) * 3600 + int(m) * 60 + int(s)) * 1000000 + int(ms),
                        int(o_price),
                        int(h_price),
                        int(l_price),
                        int(cur_price),
                        int(cur_flag),
                    ),
                )

        # Check if it is a Volumne block
        elif tag_id == b"VL":
            val_vl = struct.unpack(fmt_vl, c)
            _, _, _, flag, vol, timestamp, _ = val_vl

            h, m, s = list(struct.unpack("2s2s2s", timestamp))

            yield (
                tag_id,
                (
                    (int(h) * 3600 + int(m) * 60 + int(s)) * 1000000,
                    int(vol),
                ),
            )

        else:
            # we don't need other chunks
            pass


def _get_security_code(exchange: str, security: str) -> str:
    """Combine the exchange code and the security code
    """
    m = {
        "1": "t",  # Tokyo Stock Exchange
        "3": "n",  # Nagoya Stock Exchange
        "6": "f",  # Fukuoka Stock Exchange
        "8": "s",  # Sapporo Stock Exchange
    }
    return m.get(exchange) + security


def _dump_to_h5(stream: BytesIO, store: tables.File, file_size: int):
    """Convert and dump to h5
    Args:
        stream (InputStream) : input stream
        store (tables.File)  : pytable output
    """

    out_price = dict()
    out_volume = dict()
    
    with tqdm(total = file_size, desc = 'Streaming', unit = 'B', unit_scale = 1, ncols = 100) as pbar:
        while True:

            chunk = _load_chunk(stream)
            if chunk is None:
                break

            payload, exchange, session, category, security, chunk_size = chunk
            key = (exchange, security)

            for typ, row in _parse_chunk(payload):

                if typ == b"4P":

                    if key not in out_price:
                        out_price[key] = store.create_earray(
                            "/price",
                            _get_security_code(exchange, security),
                            obj=[list(row)],
                            createparents=True,
                        )
                    else:
                        out_price[key].append([list(row)])

                elif typ == b"VL":

                    if key not in out_volume:
                        out_volume[key] = store.create_earray(
                            "/volume",
                            _get_security_code(exchange, security),
                            obj=[list(row)],
                            createparents=True,
                        )
                    else:
                        out_volume[key].append([list(row)])
            pbar.update(chunk_size)


def _get_out_filename(src: str, out_dir: str, suffix: str) -> str:
    """
    """
    if src.endswith(".zip"):
        return os.path.join(out_dir, os.path.basename(src).replace(".zip", "{}.h5").format(suffix))
    elif src.endswith(".gz"):
        return os.path.join(out_dir, os.path.basename(src).replace(".gz", "`{}.h5").format(suffix))
    else:
        return os.path.join(out_dir, os.path.basename(src)) + "{}.h5".format(suffix)


def _resample_prices(node):
    """Resample the sequence of prices and return as a DataFrame
    """

    columns = ["time", "open", "high", "low", "current", "flag"]
    columns_dtype = {
        "time": "datetime64[us]",
        "open": np.float32,
        "high": np.float32,
        "low": np.float32,
        "current": np.float32,
        "flag": "int32",
    }
    agg = {"open": "first", "high": "max", "low": "min", "current": "last"}

    # create a dataframe
    df = pd.DataFrame(
        data=np.array(node),
        columns=columns
    ).astype(
        columns_dtype
    ).set_index(columns[0], inplace=False)

    # apply decimal points
    fixed = 10 ** df.loc[:, "flag"]
    df.loc[:, "open"] /= fixed
    df.loc[:, "high"] /= fixed
    df.loc[:, "low"] /= fixed
    df.loc[:, "current"] /= fixed

    # resample
    df = df.resample("1S").agg(agg).dropna()

    # rename "current" -> "close"
    df = df.rename(columns={"current": "close"})

    return df


def _resample_volumes(node):
    """Resample the sequence of volumes and return as a DataFrame

    The volume column is recorded as cumulative volume, so it has to be
    digitized by taking diff.
    """

    columns = ["time", "volume"]
    columns_dtype = {
        "time": "datetime64[us]",
        "volume": "int32",
    }

    # create a dataframe
    df = pd.DataFrame(
        np.array(node),
        columns=columns
    ).astype(
        columns_dtype
    ).set_index(columns[0], inplace=False)

    # calc delta
    df.loc[:, "volume"] = df.loc[:, "volume"].diff()

    # resample
    df = df.resample("1S").sum()

    return df


def _convert_and_store(z, out_filename, file_size):

    with tables.open_file(out_filename, mode="w") as store:
        _dump_to_h5(z, store, file_size)


def get_sftp_session(host: str, port: int, username: str, password: str):
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


def _stream_convert(stream, out_filename: str, mode: str):

    assert mode in ('zip', 'gz')

    if mode == "zip":
        zip_file = ZipFile(stream, allowZip64=True)

        # Hack to workaround the broken file size in the header
        file_size = zip_file.getinfo(zip_file.namelist()[0]).file_size
        if file_size < 2 ** 33:            
            zip_file.getinfo(zip_file.namelist()[0]).file_size = 2 ** 64 - 1
            file_size = 0

        # Open the first compressed file
        # (Only expecting one file inside the ZIP)
        with zip_file.open(zip_file.namelist()[0]) as z:
            _convert_and_store(z, out_filename, file_size)

    elif mode == "gz":
        with gzip.open(stream) as z:
            _convert_and_store(z, out_filename, 0)


def fetch_and_convert(sftp, src: str, out_dir: str) -> str:
    """Fetch an archive from the SFTP server and convert it into h5
    
    Args:
        sftp (SFTP Session): the sftp session
        src           (str): source path on the sftp server
        out_dir       (str): output directory
    Returns:
        filename (str)
    """

    out_filename = _get_out_filename(src, out_dir, "_raw")
    mode = os.path.splitext(src)[-1].replace(".", "")

    with sftp.open(src, "r") as f:
        _stream_convert(f, out_filename, mode)
        print("Finished Streaming.")
    return out_filename


def resample(src: str, out_filename: str):
    """Resample raw h5 file
    
    Args:
        src          (str): source file name
        out_filename (str): output file name
    """

    with pd.HDFStore(out_filename, mode="w", complevel=9) as writer, \
            tables.open_file(src, mode="r") as reader:

        for group in reader.root._f_walk_groups():

            if group._v_name == 'price':
                for node in tqdm(group._f_list_nodes(), desc = 'Resampling Prices', unit = ' Securities', ncols = 100):
                    writer.put(
                        key=node._v_pathname,
                        value=_resample_prices(node))

            elif group._v_name == 'volume':
                for node in tqdm(group._f_list_nodes(), desc = 'Resampling Volumes', unit = ' Securities', ncols = 100):
                    writer.put(
                        key=node._v_pathname,
                        value=_resample_volumes(node))
        print("Finished Resampling.")
