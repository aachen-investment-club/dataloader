# dataloader
This dataloader module provides easy API access to financial data providers.

## Setup

1. As RWTH Student get Refintiv Credentials from [EFI Lehrstuhl](https://www.efi.rwth-aachen.de/cms/efi/das-lehr-und-forschungsgebiet/~rihb/eikon/)

2. Download Refinitiv Desktop app

3. Get your api key from Refintiv Desktop app. You need to search for "Appkey" in the search bar. Keep in mind, some accounts from EFI might not have api key access, then the page shows an error!

4. Setup Configuration files in root directory of this repo like in the [Refinitiv Example Repo](https://github.com/LSEG-API-Samples/Example.DataLibrary.Python)

5. Setup finished!

## Usage

Documentation is work in progress. There might be frequent changes in near future and we plan to also include other APIs. This module is supposed to be added as submodule to a repository and then you can import and run all functions in [refinitivloader.py](src/refinitivloader.py)

```sh
git submodule add https://github.com/aachen-investment-club/dataloader.git
git submodule update --init --recursive
```

If the dataloader module is updated, you need to manually update the submodule in this repo too with the above command.

## Refintiv Resources

Further resources on using the lseg / refinitiv data api can be found in the [LSEG Repo](https://github.com/LSEG-API-Samples/Example.DataLibrary.Python/tree/lseg-data-examples/Tutorials).