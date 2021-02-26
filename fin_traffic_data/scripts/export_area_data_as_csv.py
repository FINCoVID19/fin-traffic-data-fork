import argparse
from enum import Enum, unique
import tarfile
from io import BytesIO
import codecs
import os

import h5py
import pandas as pd


@unique
class Area(Enum):
    PROVINCE = "province"
    ERVA = "erva"
    HCD = "hcd"

    def __str__(self):
        if self is Area.PROVINCE:
            return "province"
        elif self is Area.ERVA:
            return "erva"
        else:
            return "hcd"


def main():

    parser = argparse.ArgumentParser(
        description=(
            "Converts the aggregated TMS data for traffic between provinces or ERVAs to CSV form inside"
            "a bzipped tar-archive. For ERVAs,"
            "expects the input to be 'tms_between_ervas.h5'; for provinces 'tms_between_provinces.h5'"
        )
    )

    parser.add_argument("--input",
                        required=True,
                        help="Path to the aggregated_data by area input-file")
    args = parser.parse_args()

    inputpath = args.input
    # Get only the filename
    input_file = os.path.basename(inputpath)
    # Remove the extension
    input_file = input_file.split('.')[0]
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
                filename = input_file + "/" + key + '.csv'
                tinfo = tarfile.TarInfo(filename)
                tinfo.size = fileobj.tell()
                fileobj.seek(0)
                tfile.addfile(tinfo, fileobj=fileobj)


if __name__ == '__main__':
    main()
