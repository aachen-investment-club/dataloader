import os
import refinitiv.data as rd
import pandas as pd
import json


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

    with open(f'rics/rics_{index}.json', 'r') as f:
        rics = json.load(f)
    return rics


def update_data(rics: list[str], end: str, debug: bool = False) -> None:
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

    # if data folder does not exist create it
    if not os.path.exists("data"):
        os.makedirs("data")

    rd.open_session()

    for ric in rics:

        # if data exists
        df0 = pd.read_parquet(f"data/{ric}.parquet")
        latest_date = df0.index[-1]
        if debug:
            print(f"Latest date for {ric}: {latest_date}")
        fields = df0.columns.tolist()

        # only download latest date
        if latest_date < pd.Timestamp(end):
            df1 = rd.get_history(universe=[ric], fields=fields, start=latest_date, end=end)
            
            # append to existing data
            df = pd.concat([df0, df1])
            df.to_parquet(f"data/{ric}.parquet")
    
    rd.close_session()


def init_data(rics: list[str], fields: list[str], start: str, end: str, debug: bool = False) -> None:
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


    # if data folder does not exist create it
    if not os.path.exists("data"):
        os.makedirs("data")

    rd.open_session()

    for i, ric in enumerate(rics):
        
        # check if data already exists
        if os.path.exists(f"data/{ric}.parquet"):
            if debug:
                print(f"{i+1}/{len(rics)} | Data for {ric} already exists")
        
        #if not download data
        else:

            # get data
            df = rd.get_history(universe=[ric], fields=fields, start=start, end=end)
            
            if debug:
                print(f"{i+1}/{len(rics)} | Retrieved {len(df.columns)} fields for {ric}")
            
            # if fields are missing skip
            if len(df.columns) < len(fields):
                if debug:
                    print(f"{i+1}/{len(rics)} | Fields {set(fields) - set(df.columns) } are missing")
                continue

            # save data
            df.to_parquet(f"data/{ric}.parquet")

    rd.close_session()


def load_raw_data(rics: list[str]) -> dict:
    """Load raw data without preprocessing
    
    Args:
        rics (list[str]): Refintiv Instrument Codes
        
    Returns:
        dict: dictionary of DataFrames for each RIC
    """
    
    data = {}
    
    for i, ric in enumerate(rics):
        if os.path.exists(f"data/{ric}.parquet"):
            data[ric] = pd.read_parquet(f"data/{ric}.parquet")
        else:
            print(f"{i} / {len(rics)} | Data for {ric} not found")
    
    return data


def load_preprocessed_data(rics: list[str]) -> dict:
    """Load raw data, forward fill and remove missing values

    Args:
        rics (list[str]): Refintiv Instrument Codes

    Returns:
        dict: dictionary of DataFrames for each RIC
    """
    
    data = load_raw_data(rics)

    # preprocess
    remove = []
    for key, df in data.items():
        assert isinstance(df, pd.DataFrame), f"{key} is not a DataFrame"
        df.ffill(inplace=True)
        df.dropna(inplace=True)

    # remove erroneous data
    for key in remove: data.pop(key)

    return data