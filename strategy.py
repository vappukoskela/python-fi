import pandas as pd

import os

 

# ---- CONFIGURATION ----

input_file = "jarkko_14112025.txt", delimiter=";")  # Replace with your file name

chunk_size = 100000

output_folder = "chunks"

os.makedirs(output_folder, exist_ok=True)

 

# ---- STEP 1: SPLIT LARGE FILE INTO CHUNKS ----

chunk_num = 0

for chunk in pd.read_csv(input_file, chunksize=chunk_size):

    chunk_file = os.path.join(output_folder, f"chunk_{chunk_num}.csv")

    chunk.to_csv(chunk_file, index=False)

    chunk_num += 1

 

# ---- STEP 2: ANALYZE EACH CHUNK ----

summary_list = []

 

for file in os.listdir(output_folder):

    if file.endswith(".csv"):

        df = pd.read_csv(os.path.join(output_folder, file))

 

        # Ensure columns exist: symbol, action, quantity, price

        if all(col in df.columns for col in ["symbol", "action", "quantity", "price"]):

            grouped = df.groupby(["symbol", "action"]).agg({

                "quantity": "sum",

                "price": "mean"

            }).reset_index()

            summary_list.append(grouped)

 

# Combine all summaries

final_summary = pd.concat(summary_list)

final_summary = final_summary.groupby(["symbol", "action"]).agg({

    "quantity": "sum",

    "price": "mean"

}).reset_index()

 

# ---- STEP 3: Calculate Net Position and Income ----

# Pivot to separate BUY and SELL

pivot = final_summary.pivot(index="symbol", columns="action", values=["quantity", "price"]).fillna(0)

 

# Flatten columns

pivot.columns = [f"{a}_{b}" for a, b in pivot.columns]

pivot.reset_index(inplace=True)

 

# Calculate values

pivot["buy_value"] = pivot["quantity_BUY"] * pivot["price_BUY"]

pivot["sell_value"] = pivot["quantity_SELL"] * pivot["price_SELL"]

pivot["net_income"] = pivot["sell_value"] - pivot["buy_value"]

 

# Income percentage per stock

pivot["income_pct"] = (pivot["net_income"] / pivot["buy_value"].replace(0, pd.NA)) * 100

 

# ---- STEP 4: Total Income and Percentage ----

total_buy = pivot["buy_value"].sum()

total_income = pivot["net_income"].sum()

total_income_pct = (total_income / total_buy) * 100

 

# Save results

pivot.to_csv("stock_income_summary.csv", index=False)

 

print("✅ Analysis complete!")

print(f"Total Income: {total_income:.2f}")

print(f"Total Income Percentage: {total_income_pct:.2f}%")

 
