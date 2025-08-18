import pandas as pd

# === Load datasets ===
employment = pd.read_csv("data_clean/employment_clean.csv")
economic = pd.read_csv("data_clean/economic_clean.csv")
disaster = pd.read_csv("data_clean/disasters_clean.csv")
trade = pd.read_csv("data_clean/trade_clean.csv")
resilience = pd.read_csv("data_clean/resilience_clean.csv")
population = pd.read_csv("data_clean/population_clean.csv")
agriculture = pd.read_csv("data_clean/agriculture_clean.csv")
social_welfare = pd.read_csv("data_clean/social_welfare_clean.csv")

# === Standardize ===

## Employment (pivot indicators)
employment_wide = employment.pivot_table(
    index=["country", "year"], 
    columns="indicator", 
    values="value"
).reset_index()

## Social welfare (pivot indicators)
social_welfare_wide = social_welfare.pivot_table(
    index=["country", "year"], 
    columns="indicator", 
    values="value"
).reset_index()

## Economic (already wide)
economic_wide = economic.rename(columns={"country": "country", "year": "year"})

## Resilience (already wide)
resilience_wide = resilience.rename(columns={"Country": "country", "Year": "year"})

## Population (rename value column)
population_wide = population.rename(columns={"Country": "country", "Year": "year", "Value": "population"})

## Agriculture (rename value column)
agriculture_wide = agriculture.rename(columns={"Country": "country", "Year": "year", "Value": "agriculture_output"})

## Disasters (rename columns more cleanly)
disaster_wide = disaster.rename(columns={"Country": "country", "Year": "year"})

## Trade (aggregate by reporter country & year → total trade value)
trade_simple = trade.groupby(["partnerdesc", "refyear"])["primaryvalue"].sum().reset_index()
trade_simple = trade_simple.rename(columns={"partnerdesc": "country", "refyear": "year", "primaryvalue": "trade_value"})

# === Merge all datasets ===
dfs = [
    employment_wide,
    social_welfare_wide,
    economic_wide,
    resilience_wide,
    population_wide,
    agriculture_wide,
    disaster_wide,
    trade_simple
]

# Start from the first one and outer join step by step
from functools import reduce

merged = reduce(
    lambda left, right: pd.merge(left, right, on=["country", "year"], how="outer"),
    dfs
)

# === Save final dataset ===
merged.to_csv("data_clean/thematic_country_year_dataset.csv", index=False)

print("Final dataset shape:", merged.shape)
print("Columns:", merged.columns.tolist()[:20], "...")  # show first 20 columns