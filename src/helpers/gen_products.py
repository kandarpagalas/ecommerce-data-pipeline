import os
import json
from dotenv import load_dotenv
load_dotenv()

if __name__ == "__main__":
    product_folder = os.environ.get("PRODUCTS_FOLDER")

    products = []
    for file in os.listdir(product_folder):
        if ".json" not in file:
            continue

        with open(product_folder + file, "r", encoding="utf-8") as f:
            data = json.loads(f.read())

            data["name"] = data["name"].replace(" - MAYBE FIT", "").replace("Maybe", "Z106").strip()
            data["reference_id"] = data["sku"]
            data["unit_price"] = max(data["preco_prom"], data["preco_pix"])


            for key in ["imagens_produto", 
                        "produtos_relacionados", 
                        "copywriting", 
                        "img_description",
                        "_url",
                        "compre_junto",
                        "estoque",
                        "sku",
                        "preco_prom",
                        "preco_pix"
                        ]:
                del data[key]

            products.append(data)

    with open("src/helpers/data/products.json", "w", encoding="utf-8") as f:
            f.write(json.dumps(products, indent=4))