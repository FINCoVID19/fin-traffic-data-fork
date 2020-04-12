import argparse
from enum import Enum, unique
import pathlib
import tarfile
from io import BytesIO
import codecs

import h5py
import pandas as pd


@unique
class Area(Enum):
    PROVINCE = "province"
    ERVA = "erva"

    def __str__(self):
        if self is Area.PROVINCE:
            return "province"
        else:
            return "erva"


def main():

    parser = argparse.ArgumentParser(
        description=(
            "Converts the aggregated TMS data for traffic between provinces or ERVAs to CSV form inside"
            "a bzipped tar-archive. For ERVAs,"
            "expects the input to be 'tms_between_ervas.h5'; for provinces 'tms_between_provinces.h5'"
        )
    )

    parser.add_argument("--area", type=Area, required=True, help="Whether to collect erva or province-level data.")
    args = parser.parse_args()
    area = args.area
    if area is Area.ERVA:
        inputpath = 'tms_between_ervas.h5'
    elif area is Area.PROVINCE:
        inputpath = 'tms_between_provinces.h5'
    outputpath = inputpath.split('.')[0] + ".tar.bz2"

    with h5py.File(inputpath, 'r') as hf:
        tms_keys = list(hf.keys())
        with tarfile.open(outputpath, 'w:bz2') as tfile:
            for key in tms_keys:
                print(f"Exporting {key}")
                fileobj = BytesIO()
                writer_wrapper = codecs.getwriter('utf-8')(fileobj)
                df = pd.read_hdf(inputpath, key=key)
                df.to_csv(writer_wrapper)
                filename = inputpath.split('.')[0] + "/" + key + '.csv'
                tinfo = tarfile.TarInfo(filename)
                tinfo.size = fileobj.tell()
                fileobj.seek(0)
                tfile.addfile(tinfo, fileobj=fileobj)


if __name__ == '__main__':
    main()
