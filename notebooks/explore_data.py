import pandas as pd

# Read CSV
df = pd.read_csv("../data/ai4i2020.csv")

print(df.head())

print("\n")
print(df.info())

print("\nColumns:")
print(df.columns.tolist())

print("\nMissing Values")
print(df.isnull().sum())

print("\nStatistics")
print(df.describe())
