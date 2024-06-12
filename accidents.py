# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata

client = Socrata("data.nashville.gov", None)

results = client.get("6v6w-hpcw", limit=20)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)
print(results_df)