import datetime
import json
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from pydantic import BaseModel
from tqdm import tqdm

from src import DATA_DIR


class Coordinate(BaseModel):
    latitude: float
    longitude: float


def download_temperature_data(
    *, api_token: str, start: datetime.date, end: datetime.date, progress: bool = False,
) -> pd.DataFrame:
    """Download temperature data from darksky API

    Sign up for a free account:
    https://darksky.net/dev/register

    Arguments
    ---------
    api_token: str
        Darksky API token (secret)
    start: datetime.date
        start date of weather data timeseries
    end: datetime.date
        end date of weather data timeseries
    progress: bool
        if True, displays a progress bar

    """
    coord = Coordinate(latitude=51.509865, longitude=-0.118092)

    n_days = (end - start).days

    dir_ = DATA_DIR / "darksky"
    dir_.mkdir(parents=True, exist_ok=True)

    pbar = tqdm(total=n_days)

    frames = []

    def _fetch(i):
        date = start + datetime.timedelta(days=i)
        ts = date.isoformat()

        path = dir_ / (ts + ".json")
        url = f"https://api.darksky.net/forecast/{api_token}/{coord.latitude},{coord.longitude},{ts}?units=si"

        if not path.exists():
            urllib.request.urlretrieve(url, filename=str(path))

        item = json.loads(path.read_bytes())
        df = pd.DataFrame(item["hourly"]["data"])

        df.time = df.time.apply(datetime.datetime.fromtimestamp)
        df.temperature = pd.to_numeric(df.temperature, downcast="float")

        df = df.set_index(df.time).sort_index().drop(columns=["time"])
        df = df[["temperature"]]

        df.index.name = None

        hh_index = pd.date_range(
            start=df.index.min(),
            end=df.index.max() + datetime.timedelta(minutes=30),
            freq="30T",
        )

        df = hh_index.to_frame().join(df).fillna(method="ffill").drop(columns=[0])
        return df

    with ThreadPoolExecutor() as pool:
        for frame in pool.map(_fetch, range(n_days)):
            frames.append(frame)
            if progress:
                pbar.update(1)

    f = pd.concat(frames)

    pbar.close()
    urllib.request.urlcleanup()

    return f
