import re
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

base_dir = Path(__file__).parent
data_dir = base_dir / "data"


def low_carbon_london():
    start = time.time()

    data_dir.mkdir(exist_ok=True)

    raw = data_dir / "low-carbon-london.zip"
    extracted = data_dir / "low-carbon-london"

    if not raw.exists():
        t0 = time.time()
        print("- Downloading Low Carbon London dataset... ", end="", flush=True)
        url = "https://data.london.gov.uk/download/smartmeter-energy-use-data-in-london-households/04feba67-f1a3-4563-98d0-f3071e3d56d1/Power-Networks-LCL-June2015(withAcornGps).csv_Pieces.zip"
        urllib.request.urlretrieve(url, raw)
        t1 = time.time()
        print("done in {:0.2f}s".format(t1 - t0), flush=True)

    if not extracted.exists():
        t0 = time.time()
        print("- Extracting CSV files from zip... ", end="", flush=True)

        data = zipfile.ZipFile(raw)
        infos = [info for info in data.infolist() if not info.is_dir()]

        for info in infos:
            info.filename = re.search(r"v2_([0-9]+\.csv)", info.filename).group(1)
            data.extract(info, path=extracted)

        t1 = time.time()
        print("done in {:0.2f}s".format(t1 - t0), flush=True)

    end = time.time()
    print("** Created LCL dataset in {:0.2f}s **".format(end - start))


if __name__ == "__main__":
    sys.exit(low_carbon_london())
