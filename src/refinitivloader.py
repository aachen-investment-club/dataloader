import os
import lseg.data as ld
import pandas as pd
import json
from pathlib import Path

def open_session() -> None:
    """Open Refinitiv session

    Example:
    >>> open_session()

    """
    ld.open_session(
    config_name="Configuration/refinitiv-data.config.json",
    name="platform.rdp")


def get_rics(index: str) -> list[str]:
    """Get RICs for a given index

    Args:
        index (str): Index name
    
    Returns:
        list[str]: List of RICs

    Example:
    >>> index = "sp500"
    >>> rics = get_rics(index)
    >>> print(rics)

    Available indices:
    - sp500
    """

    with open(f'dataloader/naming/rics_{index}.json', 'r') as f:
        rics = json.load(f)
    return rics

def get_fields() -> list[dict]:
    """Get list of fields available for download

    Returns:
        list[str]: List of fields

    Example:
    >>> fields = get_fields()
    >>> print(fields)
    """

    with open('dataloader/naming/fields.json', 'r') as f:
        fields = json.load(f)
    return fields


def update_data(rics: list[str], new_end: str, base_dir: Path, debug: bool = False) -> None:
    """Update latest data and concatenate to existing data

    Args:
        rics (list[str]): Refinitiv Instrument Codes
        end (str): End date for data
        debug (bool, optional): Print debug information. Defaults to False.
    
    Example:
    >>> rics = ["AAPL.O", "MSFT.O"]
    >>> end = "2021-12-31"
    >>> update_data(rics, end, debug=True)

    """

    if not isinstance(base_dir, Path):
        base_dir = Path(base_dir)

    # if data folder does not exist create it
    if not os.path.exists("data"):
        os.makedirs("data")

    open_session()

    for i, ric in enumerate(rics):

        # if data exists
        df0 = pd.read_parquet(base_dir / f"{ric}.parquet")
        latest_date = df0.index[-1]
        if debug:
            print(f"Latest date for {ric}: {latest_date}")
        cols = df0.columns.tolist()
        
        available_fields = get_fields()

        fields = []
        for field_name in cols:
            if field_name not in available_fields:
                print(f"Field {field_name} not available for {ric}")
                return
            else:
                fields.append(available_fields[field_name])

        # only download latest date
        if latest_date < pd.Timestamp(new_end):
            df1 = ld.get_history(universe=[ric], fields=fields, start=latest_date, end=new_end)
            
            # append to existing data
            df = pd.concat([df0, df1])
            df.to_parquet(base_dir / f"{ric}.parquet")

            if debug:
                print(f"{(i+1)}/{len(rics)} | Downloaded data for {ric}")
    
    ld.close_session()


def init_data(rics: list[str], fields: list[str], start: str, end: str, base_dir: Path, debug: bool = False) -> None:
    """Download data for a list of RICs and save to parquet files

    Args:
        rics (list[str]): Refinitiv Instrument Codes
        fields (list[str]): List of fields to download
        start (str): Start date for data
        end (str): End date for data
        debug (bool, optional): Print debug information. Defaults to False.
    
    Example:
    >>> rics = ["AAPL.O", "MSFT.O"]
    >>> fields = ["TRDPRC_1"]
    >>> start = "2020-01-01"
    >>> end = "2021-12-31"
    >>> init_data(rics, fields, start, end, debug=True)
    
    """

    if not isinstance(base_dir, Path):
        base_dir = Path(base_dir)

    # if data folder does not exist create it
    if not os.path.exists("data"):
        os.makedirs("data")

    # check which rics already have data
    skip = []
    for ric in rics:
        if os.path.exists(base_dir / f"{ric}.parquet"):
            skip.append(ric)
            if debug:
                print(f"Data for {ric} already exists")

    # remove rics that already have data
    rics = list(set(rics) - set(skip))

    # if no rics to download return
    if len(rics) == 0:
        return
    
    # open session and download data
    open_session()

    for i, ric in enumerate(rics):
    
        # get data
        df = ld.get_history(universe=[ric], fields=fields, start=start, end=end)
        
        if debug:
            print(f"{i+1}/{len(rics)} | Retrieved {len(df.columns)} fields for {ric}")
        
        # if fields are missing skip
        if len(df.columns) < len(fields):
            if debug:
                print(f"{i+1}/{len(rics)} | Fields {set(fields) - set(df.columns) } are missing")
            continue

        # save data
        df.to_parquet(base_dir / f"{ric}.parquet")

    ld.close_session()


def load_raw_data(rics: list[str], base_dir: Path) -> dict:
    """Load raw data without preprocessing
    
    Args:
        rics (list[str]): Refintiv Instrument Codes
        
    Returns:
        dict: dictionary of DataFrames for each RIC
    """
    
    if not isinstance(base_dir, Path):
        base_dir = Path(base_dir)

    data = {}
    
    for i, ric in enumerate(rics):
        ric_dir = base_dir / f"{ric}.parquet"
        if os.path.exists(ric_dir):
            data[ric] = pd.read_parquet(ric_dir)
        else:
            print(f"{i} / {len(rics)} | Data for {ric} not found")
    
    return data


def load_preprocessed_data(rics: list[str], base_dir: Path) -> dict:
    """Load raw data, forward fill and remove missing values

    Args:
        rics (list[str]): Refintiv Instrument Codes

    Returns:
        dict: dictionary of DataFrames for each RIC
    """
    
    if not isinstance(base_dir, Path):
        base_dir = Path(base_dir)

    data = load_raw_data(rics, base_dir)

    # preprocess
    remove = []
    for key, df in data.items():
        assert isinstance(df, pd.DataFrame), f"{key} is not a DataFrame"
        df.ffill(inplace=True)
        df.dropna(inplace=True)

    # remove erroneous data
    for key in remove: data.pop(key)

    return data