# -*- coding: utf-8 -*-

from zipfile import ZipFile
import datetime
import gzip
import numpy as np
import os
import pandas as pd
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


def _parse_chunk(payload: bytes, date_offset_epoch: int) -> tuple:
    """Parse the chunk and yields for each tag

    Extract `4P` and `VL` tags. Detailed explanations can be found in

    "04_Market Information System FLEX Connection Specification Index¥Statistics group DS.15.10.pdf"

    Args:
        payload     (str) : payload of the chunk
        date_offset (int) : base date offet in epoch
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
                closing_flag,
            ) = val_4p

            # skip invalid price
            if closing_flag == b'1':
                continue

            if len(cur_timestamp.strip()) > 0:
                h, m, s, ms = list(struct.unpack("2s2s2s6s", cur_timestamp))

                yield (
                    tag_id,
                    (
                        (date_offset_epoch + int(h) * 3600 + int(m) * 60 + int(s))
                        * 1000000
                        + int(ms),
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
                    (date_offset_epoch + int(h) * 3600 + int(m) * 60 + int(s))
                    * 1000000,
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
    if exchange not in m:
        raise ValueError("Unknown Exchange code", exchange)
    return m.get(exchange) + security


def _dump_to_h5(
    stream: BytesIO, store: tables.File, file_size: int, date: datetime.date
):
    """Convert and dump to h5
    Args:
        stream (InputStream) : input stream
        store (tables.File)  : pytable output
        file_size (int)      : size of the file
        date (date)          : date of the file
    """

    out_price = dict()
    out_volume = dict()

    date_offset_epoch = datetime.datetime.fromordinal(date.toordinal()).timestamp()

    with tqdm(
        total=file_size, desc="Streaming", unit="B", unit_scale=1, ncols=100
    ) as pbar:
        while True:

            chunk = _load_chunk(stream)
            if chunk is None:
                break

            payload, exchange, session, category, security, chunk_size = chunk
            key = (exchange, security)

            for typ, row in _parse_chunk(payload, date_offset_epoch):

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


def _get_outpath(src: str, suffix: str) -> str:
    if src.endswith(".zip"):
        return os.path.join(
            os.path.dirname(src),
            os.path.basename(src).replace(".zip", "{}.h5").format(suffix),
        )
    elif src.endswith(".gz"):
        return os.path.join(
            os.path.dirname(src),
            os.path.basename(src).replace(".gz", "{}.h5").format(suffix),
        )
    else:
        raise ValueError("Unsupported suffix: {}".format(src))


def _extract_prices(node):
    """Extract the sequence of prices and return as a Series
    """

    columns = ["time", "current", "flag"]
    columns_dtype = {
        "time": "datetime64[us]",
        "current": np.float32,
        "flag": "int32",
    }

    # create a dataframe
    df = (
        pd.DataFrame(data=np.array(node), columns=columns)
        .astype(columns_dtype)
        .set_index(columns[0], inplace=False)
    ).sort_index()

    # apply decimal points
    fixed = 10 ** df.loc[:, "flag"]
    df.loc[:, "current"] /= fixed

    return df["current"]


def _extract_volumes(node):
    """Extract the sequence of volumes and return as a Series

    The volume column is recorded as cumulative volume, so it has to be
    digitized by taking diff.
    """

    columns = ["time", "volume"]
    columns_dtype = {"time": "datetime64[us]", "volume": "int32"}

    # create a dataframe
    df = (
        pd.DataFrame(np.array(node), columns=columns)
        .astype(columns_dtype)
        .set_index(columns[0], inplace=False)
    )

    # calc delta
    initial_volume = df["volume"].iloc[0]
    df.loc[:, "volume"] = df.loc[:, "volume"].diff()
    df.iloc[0] = initial_volume

    return df["volume"]


def _resample_ohlc(price, volume, freq):
    return pd.concat([
        price.resample(freq).ohlc(),
        volume.resample(freq).sum().to_frame("volume"),
        (price * volume.values).resample(freq).sum().to_frame("amount"),
    ], axis=1)


def _convert_and_store(z, outpath, file_size, date):

    with tables.open_file(outpath, mode="w") as store:
        _dump_to_h5(z, store, file_size, date)


def _stream_convert(stream, outpath: str, mode: str, date: str):

    assert mode in ("zip", "gz")

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
            _convert_and_store(z, outpath, file_size, date)

    elif mode == "gz":
        with gzip.open(stream) as z:
            _convert_and_store(z, outpath, 0, date)


def _extract_date(filename: str):
    return datetime.datetime.strptime(
        os.path.basename(filename), "StandardEquities_%Y%m%d.zip"
    )


def fetch_and_convert(src: str, suffix: str = "") -> str:
    """Fetch an archive and convert it into h5

    Args:
        src      (str): source path of the raw zip file
    Returns:
        filename (str)
    """

    outpath = _get_outpath(src, suffix)
    mode = os.path.splitext(src)[-1].replace(".", "")

    date = _extract_date(src)

    _stream_convert(src, outpath, mode, date)

    return outpath


def resample(src: str, outpath: str, freq: str):
    """Resample raw h5 file

    Args:
        src     (str): source file name
        outpath (str): output file name
        freq    (str): frequency of resampling
    """

    with pd.HDFStore(outpath, mode="w", complevel=9) as writer, tables.open_file(
        src, mode="r"
    ) as reader:

        for group in reader.root._f_walk_groups():

            if group._v_name == "price":
                for node_prices in tqdm(
                    group._f_list_nodes(),
                    desc="Resampling",
                    unit=" Securities",
                    ncols=100,
                ):
                    # combine prices and volumes
                    node_volumes = reader.get_node(
                        node_prices._v_pathname.replace("price", "volume")
                    )

                    # resample
                    df = _resample_ohlc(
                        _extract_prices(node_prices),
                        _extract_volumes(node_volumes),
                        freq)

                    writer.put(key=node_prices._v_name, value=df)
