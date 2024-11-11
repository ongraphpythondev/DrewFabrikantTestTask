import pandas as pd
from utils import split_name, standardize_name, get_address, clean_dataframe
from mappings import column_mapping_agents, column_mapping_agents_2,column_mapping_all_agents

## EXCEL FILE PATHS
INPUTFOLDER = "Input/"
AGENTS = f"{INPUTFOLDER}agents.xlsx"
AGENTS2 = f"{INPUTFOLDER}agents_2.xlsx"
ALL_AGENTS = f"{INPUTFOLDER}all_agents.xlsx"
TRANSACTIONS = f"{INPUTFOLDER}transactions.xlsx"

## DESTINATION PATH
TEAM_PERFORMANCE = "result/team_performance.xlsx"
AGENT_PERFORMANCE = "result/agent_performance.xlsx"
NORMALISED_BROKERAGE = "result/normalized_brokerage.xlsx"


# Load data from Excel files
print("Reading Input Excel Files............")
agents_df = pd.read_excel(AGENTS)
agents_2_df = pd.read_excel(AGENTS2)
all_agents_df = pd.read_excel(ALL_AGENTS)
transactions_df = pd.read_excel(TRANSACTIONS)
print("Done Reading Excel Files")
columns_to_standardize_agents = {
    "full_name": lambda x: x.title() if isinstance(x, str) else x,
    "email": lambda x: x.lower() if isinstance(x, str) else x,
    "office_name": lambda x: x.upper() if isinstance(x, str) else x,
}

columns_to_standardize_agents_2 = {
    "agentdetails__mlsagentfullname": lambda x: x.title() if isinstance(x, str) else x,
    "agentdetails__email": lambda x: x.lower() if isinstance(x, str) else x,
    "agentdetails__office__name": lambda x: x.upper() if isinstance(x, str) else x,
}


columns_to_standardize_all_agents = {
    "Name": lambda x: x.title() if isinstance(x, str) else x,
    "Email": lambda x: x.lower() if isinstance(x, str) else x,
    "Org": lambda x: x.upper() if isinstance(x, str) else x,
}


columns_to_standardize_transactions = {
    "presented_by": lambda x: x.title() if isinstance(x, str) else x,
    "agent_email": lambda x: x.lower() if isinstance(x, str) else x,
    "brokered_by": lambda x: x.upper() if isinstance(x, str) else x,
}

# Normalize email addresses, fullname, office_name for consistent merging
print("Starting Cleaning.........")
agents_df = clean_dataframe(
    agents_df, columns_to_standardize=columns_to_standardize_agents
)
agents_2_df = clean_dataframe(
    agents_2_df, columns_to_standardize=columns_to_standardize_agents_2
)
all_agents_df = clean_dataframe(
    all_agents_df, columns_to_standardize=columns_to_standardize_all_agents
)
transactions_df = clean_dataframe(
    transactions_df, columns_to_standardize=columns_to_standardize_transactions
)


agents_2_df["full_address"] = agents_2_df.apply(
    lambda row: ", ".join(
        filter(
            lambda x: pd.notnull(x) and x != "" and x != "nan",
            map(
                str,
                [
                    row["agentdetails__office__fullstreetaddress"],
                    row["agentdetails__office__city"],
                    row["agentdetails__office__state"],
                    row["agentdetails__office__zip"],
                ],
            ),
        )
    ),
    axis=1,
)

all_agents_df["full_address"] = all_agents_df.apply(
    lambda row: ", ".join(
        filter(
            lambda x: pd.notnull(x) and x != "" and x != "nan",
            map(str, [row["Full_address"]]),
        )
    ),
    axis=1,
)
transactions_df["full_address"] = transactions_df.apply(
    lambda row: ", ".join(
        filter(
            lambda x: pd.notnull(x) and x != "" and x != "nan",
            map(
                str,
                [
                    row["address_line_1"],
                    row["address_line_2"],
                    row["city"],
                    row["state"],
                    row["zip_code"],
                ],
            ),
        )
    ),
    axis=1,
)
agents_df["full_address"] = agents_df.apply(
    lambda row: ", ".join(
        filter(
            lambda x: pd.notnull(x) and x != "" and x != "nan",
            map(
                str,
                [
                    row["office_address_1"],
                    row["office_address_2"],
                    row["office_city"],
                    row["office_state"],
                    row["office_zip"],
                    row["office_county"],
                ],
            ),
        )
    ),
    axis=1,
)


agents_2_df[
    ["AddressNumber", "StreetName", "PlaceName", "StateName", "ZipCode", "CountryName"]
] = agents_2_df["full_address"].apply(lambda x: pd.Series(get_address(x)))


all_agents_df[
    ["AddressNumber", "StreetName", "PlaceName", "StateName", "ZipCode", "CountryName"]
] = all_agents_df["full_address"].apply(lambda x: pd.Series(get_address(x)))

agents_df[
    ["AddressNumber", "StreetName", "PlaceName", "StateName", "ZipCode", "CountryName"]
] = agents_df["full_address"].apply(lambda x: pd.Series(get_address(x)))

transactions_df[
    ["AddressNumber", "StreetName", "PlaceName", "StateName", "ZipCode", "CountryName"]
] = transactions_df["full_address"].apply(lambda x: pd.Series(get_address(x)))


# Apply column mappings for consistency
agents_df.rename(columns=column_mapping_agents, inplace=True)
agents_2_df.rename(columns=column_mapping_agents_2, inplace=True)
all_agents_df.rename(columns=column_mapping_all_agents, inplace=True)

# Merge agents dataframes on 'email' column
merged_agents_1 = pd.merge(agents_df, agents_2_df, on="email", how="outer")
unified_agents_df = pd.merge(merged_agents_1, all_agents_df, on="email", how="outer")


# Clean and unify agent names
unified_agents_df["fullname"] = (
    unified_agents_df["full_name"]
    .fillna(unified_agents_df["full_name_x"].fillna(unified_agents_df["full_name_y"]))
    .apply(lambda x: x.title() if pd.notnull(x) else x)
)


# Clean and unify first, middle, and last names
unified_agents_df["first_name"] = (
    unified_agents_df["first_name_x"]
    .fillna(unified_agents_df["first_name_y"])
    .apply(lambda x: str(x).title() if pd.notnull(x) else x)
)

unified_agents_df["middle_name"] = (
    unified_agents_df["middle_name_x"]
    .fillna(unified_agents_df["middle_name_y"])
    .apply(lambda x: str(x).title() if pd.notnull(x) else x)
)

unified_agents_df["last_name"] = (
    unified_agents_df["last_name_x"]
    .fillna(unified_agents_df["last_name_y"])
    .apply(lambda x: str(x).title() if pd.notnull(x) else x)
)

unified_agents_df[["first_name", "middle_name", "last_name"]] = unified_agents_df[
    "fullname"
].apply(lambda x: pd.Series(split_name(x)))

# Clean Organisation data
print("Organising Cleaned Data...............")
unified_agents_df["org_name"] = unified_agents_df["office_name"].fillna(
    unified_agents_df["office_name_x"].fillna(unified_agents_df["office_name_y"])
)
unified_agents_df["ZipCode"] = unified_agents_df["ZipCode_x"].fillna(
    unified_agents_df["ZipCode_y"]
)
unified_agents_df["CountryName"] = unified_agents_df["CountryName_x"].fillna(
    unified_agents_df["CountryName_y"]
)
unified_agents_df["screen_name"] = unified_agents_df["screen_name_x"].fillna(
    unified_agents_df["screen_name_y"]
)
unified_agents_df["org_phone"] = unified_agents_df["office_phone"].fillna(
    unified_agents_df["office_phone_x"].fillna(unified_agents_df["office_phone_y"])
)
unified_agents_df["org_fax"] = unified_agents_df["office_fax_x"].fillna(
    unified_agents_df["office_fax_y"]
)
unified_agents_df["cell"] = unified_agents_df["cell_phone"].fillna(
    unified_agents_df["cell_phone_x"].fillna(unified_agents_df["cell_phone_y"])
)
unified_agents_df["license_no"] = unified_agents_df["license_number"].fillna(
    unified_agents_df["license_number_x"].fillna(unified_agents_df["license_number_y"])
)
unified_agents_df["agent_id"] = unified_agents_df["agent_id_x"].fillna(
    unified_agents_df["agent_id_y"]
)
unified_agents_df["full_address"] = unified_agents_df["full_address_x"].fillna(
    unified_agents_df["full_address_y"]
)
unified_agents_df["AddressNumber"] = unified_agents_df["AddressNumber_x"].fillna(
    unified_agents_df["AddressNumber_y"]
)
unified_agents_df["StreetName"] = unified_agents_df["StreetName_x"].fillna(
    unified_agents_df["StreetName_y"]
)
unified_agents_df["PlaceName"] = unified_agents_df["PlaceName_x"].fillna(
    unified_agents_df["PlaceName_y"]
)
unified_agents_df["StateName"] = unified_agents_df["StateName_x"].fillna(
    unified_agents_df["StateName_y"]
)



#here we can do if the column name ends with _x and _y then drop and otherthan we can manually drop like full_name
unified_agents_df = unified_agents_df.drop(
    columns=[
        "full_name_x",
        "full_name_y",
        "full_name",
        "first_name_x",
        "first_name_y",
        "middle_name_x",
        "middle_name_y",
        "last_name_x",
        "last_name_y",
        "office_address_x",
        "office_address_2",
        "office_address_y",
        "office_address",
        "office_name_x",
        "office_name_y",
        "office_name",
        "office_city_x",
        "office_city_y",
        "office_city",
        "office_state_x",
        "office_state_y",
        "office_state",
        "office_zip_x",
        "office_zip_y",
        "office_state",
        "office_phone_x",
        "office_phone_y",
        "office_phone",
        "office_fax_x",
        "office_fax_y",
        "cell_phone_x",
        "cell_phone_y",
        "cell_phone",
        "license_number_x",
        "license_number_y",
        "license_number",
        "agent_id_x",
        "agent_id_y",
        "office_county",
        "screen_name_x",
        "screen_name_y",
        "StateName_x",
        "StateName_y",
        "PlaceName_x",
        "PlaceName_y",
        "StreetName_y",
        "StreetName_x",
        "AddressNumber_x",
        "AddressNumber_y",
        "full_address_x",
        "full_address_y",
        "ZipCode_x",
        "ZipCode_y",
        "CountryName_y",
        "CountryName_x",
        "screen_name_x",
        "screen_name_y",
    ]
)

## MERGING WITH TRANSACTIONS
transactions_df = transactions_df.rename(columns={"agent_email": "email"})

merged_df = pd.merge(unified_agents_df, transactions_df, on="email", how="inner")
merged_df["team_name"] = merged_df["team_name_x"].fillna(merged_df["team_name_y"])
merged_df = merged_df.drop(columns=["team_name_x", "team_name_y"])

email_set = set(transactions_df["email"].dropna())
transactions_df["temp_key"] = transactions_df.apply(
    lambda row: (
        row["email"]
        if pd.notnull(row["email"])
        else f"{row['brokered_by']}_{row['presented_by']}"
    ),
    axis=1,
)

unified_agents_df["temp_key"] = unified_agents_df.apply(
    lambda row: (
        row["email"]
        if row["email"] in email_set
        else f"{row['org_name']}_{row['fullname']}"
    ),
    axis=1,
)

merged_df = pd.merge(
    unified_agents_df,
    transactions_df,
    on="temp_key",
    how="inner",
    suffixes=("_agents", "_transactions"),
)

merged_df["email"] = merged_df["email_agents"].fillna(merged_df["email_transactions"])
merged_df["team_name"] = merged_df["team_name_agents"].fillna(
    merged_df["team_name_transactions"]
)
merged_df = merged_df.drop(
    columns=[
        "temp_key",
        "email_agents",
        "email_transactions",
        "team_name_agents",
        "team_name_transactions",
    ]
)


# merged_df = merged_df.loc[merged_df['status'] == 'Sold']


## TEAM PERFORMANCE
# Calculate agent performance metrics
team_performance = (
    merged_df.groupby(["team_name"])
    .agg(
        team_total_sales=("price", "sum"),
        avg_team_sale_price=("price", "mean"),
        team_transaction_count=("id", "count"),
        avg_team_square_feet=("square_feet", "mean"),
        total_team_square_feet=("square_feet", "sum"),
        avg_team_rating=("rating", "mean"),
        sales_percentage=("price", lambda x: x.sum() / merged_df["price"].sum() * 100),
    )
    .reset_index()
)


team_performance["avg_team_sale_price"] = team_performance["avg_team_sale_price"].round(
    0
)
team_performance["avg_team_square_feet"] = team_performance[
    "avg_team_square_feet"
].round(2)
team_performance["avg_team_rating"] = team_performance["avg_team_rating"].round(1)
team_performance["sales_percentage"] = team_performance["sales_percentage"].round(2)

property_type_count = pd.crosstab(merged_df["team_name"], merged_df["property_type"])
property_type_count = property_type_count.reset_index()
team_performance.index = team_performance.index + 1
team_performance.to_excel(TEAM_PERFORMANCE, sheet_name="Team Performance")

with pd.ExcelWriter(TEAM_PERFORMANCE, mode="a", engine="openpyxl") as writer:
    property_type_count.to_excel(
        writer, sheet_name="Sale Count By Property Type", index=False
    )


# AGENT PERFORMANCE
agent_performance = (
    merged_df.groupby(["fullname"])
    .agg(
        total_sales=("price", "sum"),
        avg_sale_price=("price", "mean"),
        transaction_count=("id", "count"),
        avg_square_feet=("square_feet", "mean"),
        total_square_feet=("square_feet", "sum"),
        avg_rating=("rating", "mean"),
        reviews_count=("reviews", "count"),
        sales_percentage=("price", lambda x: x.sum() / merged_df["price"].sum() * 100),
    )
    .reset_index()
)

agent_performance["avg_sale_price"] = agent_performance["avg_sale_price"].round(0)
agent_performance["avg_square_feet"] = agent_performance["avg_square_feet"].round(2)
agent_performance["avg_rating"] = agent_performance["avg_rating"].round(1)
agent_performance["sales_percentage"] = agent_performance["sales_percentage"].round(2)

property_type_count = pd.crosstab(merged_df["fullname"], merged_df["property_type"])
property_type_count = property_type_count.reset_index()

agent_performance.index = agent_performance.index + 1
agent_performance.to_excel(AGENT_PERFORMANCE, sheet_name="Agent Performance")

with pd.ExcelWriter(AGENT_PERFORMANCE, mode="a", engine="openpyxl") as writer:
    property_type_count.to_excel(
        writer, sheet_name="Sale Count By Property Type", index=False
    )


## Normalized Brokerage Information
from sklearn.preprocessing import MinMaxScaler

# Aggregate data at the brokerage level
brokerage_info = (
    merged_df.groupby(["brokered_by"])
    .agg(
        brokerage_total_sales=("price", "sum"),  # Total sales by brokerage
        avg_brokerage_sale_price=("price", "mean"),  # Average sale price per brokerage
        brokerage_transaction_count=("id", "count"),  # Transaction count by brokerage
        active_listings_count=(
            "status",
            lambda x: (x == "Active").sum(),
        ),  # Active listings
        sales_percentage=("price", lambda x: x.sum() / merged_df["price"].sum() * 100),
    )
    .reset_index()
)

# Normalize selected columns
scaler = MinMaxScaler()
brokerage_info[
    ["normalized_total_sales", "normalized_avg_price", "normalized_transaction_count"]
] = scaler.fit_transform(
    brokerage_info[
        [
            "brokerage_total_sales",
            "avg_brokerage_sale_price",
            "brokerage_transaction_count",
        ]
    ]
)

brokerage_info["avg_brokerage_sale_price"] = brokerage_info[
    "avg_brokerage_sale_price"
].round(0)
brokerage_info["sales_percentage"] = brokerage_info["sales_percentage"].round(2)
brokerage_info["normalized_avg_price"] = brokerage_info["normalized_avg_price"].round(2)
brokerage_info["normalized_transaction_count"] = brokerage_info[
    "normalized_transaction_count"
].round(2)
brokerage_info["normalized_total_sales"] = brokerage_info[
    "normalized_total_sales"
].round(2)

property_type_count = pd.crosstab(merged_df["brokered_by"], merged_df["property_type"])
property_type_count = property_type_count.reset_index()

brokerage_info.index = brokerage_info.index + 1
brokerage_info.to_excel(NORMALISED_BROKERAGE, sheet_name="Brokerage Performance")

with pd.ExcelWriter(NORMALISED_BROKERAGE, mode="a", engine="openpyxl") as writer:
    property_type_count.to_excel(
        writer, sheet_name="Sale Count By Property Type", index=False
    )
print("Process Completed. The Output Files should be in the 'result' Folder..")