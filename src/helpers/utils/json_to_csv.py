import json
import pandas as pd


def parse_json(json_file):
    csv_file_name = json_file.replace(".json", ".csv")
    parquet_file_name = json_file.replace(".json", ".parquet")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.loads(f.read())

        df = pd.DataFrame(data)
        print(df.sample(5))
        df.to_parquet(parquet_file_name)
        df.to_csv(csv_file_name, index=False)


if __name__ == "__main__":
    files = ["products.json", "customers.json"]

    for file in files:
        parse_json("src/data/" + file)
