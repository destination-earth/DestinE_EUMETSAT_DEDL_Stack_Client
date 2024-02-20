# DestinE Data Lake (DEDL) Stack Client

Python client to facilitate the use of DestinE Data Lake Stack Service.

## Installation

Install the client library with pip:
```shell
pip install dedl-stack-client
```

## Usage
### Dask
An example notebook is provided [here](./examples/client-usage.ipynb).
The client will guide a user through the needed authentication flow and will automatically create dedicated Dask cluster on each DEDL bridge.

```python
from dedl_stack_client.authn import DaskOIDC
from dedl_stack_client.dask import DaskMultiCluster
from rich.prompt import Prompt

myAuth = DaskOIDC(username=Prompt.ask(prompt="Username"))
myDEDLClusters = DaskMultiCluster(auth=myAuth)
myDEDLClusters.new_cluster()
```

The DaskMultiCluster class provides an abstraction layer to interact with the various clusters on each DEDL bridge. Computations can be directed to the different Dask clusters by making use of a context manager as given in the following.

```python
with myDEDLClusters.as_current(location="central") as myclient:
    myclient.compute(myarray)
with myDEDLClusters.as_current(location="lumi") as myclient:
    myclient.compute(myarray)
```
