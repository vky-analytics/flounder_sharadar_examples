# cython-based interface for reading the bcolz bundle
# implemented from the corresponding method within
# zipline/data/_equities.pyx for supporting float64 values
# and sharadar daily tags.

import bcolz
cimport cython
from cpython cimport bool

from numpy import (
    array,
    float64,
    full,
    intp,
    uint32,
    int64,
    zeros,
    iinfo
)
from numpy cimport (
    float64_t,
    intp_t,
    ndarray,
    uint32_t,
    int64_t,
    uint8_t,
)
from numpy.math cimport NAN

from fsharadar.defs import bundle_missing_value

ctypedef object carray_t
ctypedef object ctable_t
ctypedef object Timestamp_t
ctypedef object DatetimeIndex_t
ctypedef object Int64Index_t

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef _read_bcolz_data(ctable_t table,
                       tuple shape,
                       list columns,
                       intp_t[:] first_rows,
                       intp_t[:] last_rows,
                       intp_t[:] offsets,
                       bool read_all,
		       bundle_tags): # new argument
    """
    Load raw bcolz data for the given columns and indices.

    Parameters
    ----------
    table : bcolz.ctable
        The table from which to read.
    shape : tuple (length 2)
        The shape of the expected output arrays.
    columns : list[str]
        List of column names to read.

    first_rows : ndarray[intp]
    last_rows : ndarray[intp]
    offsets : ndarray[intp
        Arrays in the format returned by _compute_row_slices.
    read_all : bool
        Whether to read_all sid data at once, or to read a silce from the
        carray for each sid.

    Returns
    -------
    results : list of ndarray
        A 2D array of shape `shape` for each column in `columns`.
    """
    cdef:
        int nassets
        str column_name
        carray_t carray
        ndarray[dtype=float64_t, ndim=1] raw_data
        ndarray[dtype=float64_t, ndim=2] outbuf
        ndarray[dtype=uint8_t, ndim=2, cast=True] where_nan
        ndarray[dtype=float64_t, ndim=2] outbuf_as_float
        intp_t asset
        intp_t out_idx
        intp_t raw_idx
        intp_t first_row
        intp_t last_row
        intp_t offset
        list results = []

    ndays = shape[0]
    nassets = shape[1]
    if not nassets== len(first_rows) == len(last_rows) == len(offsets):
        raise ValueError("Incompatible index arrays.")

    for column_name in columns:
        outbuf = zeros(shape=shape, dtype=float64)
        if read_all:
            raw_data = table[column_name][:]

            for asset in range(nassets):
                first_row = first_rows[asset]
                if first_row == -1:
                    # This is an unknown asset, leave its slot empty.
                    continue

                last_row = last_rows[asset]
                offset = offsets[asset]

                if first_row <= last_row:
                    outbuf[offset:offset + (last_row + 1 - first_row), asset] =\
                        raw_data[first_row:last_row + 1]
                else:
                    continue
        else:
            carray = table[column_name]

            for asset in range(nassets):
                first_row = first_rows[asset]
                if first_row == -1:
                    # This is an unknown asset, leave its slot empty.
                    continue

                last_row = last_rows[asset]
                offset = offsets[asset]
                out_start = offset
                out_end = (last_row - first_row) + offset + 1
                if first_row <= last_row:
                    outbuf[offset:offset + (last_row + 1 - first_row), asset] =\
                        carray[first_row:last_row + 1]
                else:
                    continue

        if column_name in set(bundle_tags):
            outbuf_as_float = outbuf.astype(float64) 
            where_nan = (outbuf_as_float == bundle_missing_value)
            outbuf_as_float[where_nan] = NAN
            results.append(outbuf_as_float)
        else:
            results.append(outbuf)

    return results
