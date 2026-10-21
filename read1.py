#Write Python code to load the file into a Pandas DataFrame, calculate the total sales for each product, and save the results into a new CSV file. Click in the link below to access sales.csv dataset.

import pandas as pd
df = pd.read_csv("sales.csv")

totals = df.groupby(["Product"]) ["Sales"].sum()

totals.to_csv('totals.csv')