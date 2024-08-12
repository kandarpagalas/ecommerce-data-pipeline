import uuid
import pendulum
from random import randint
from faker import Faker

fake = Faker("pt_BR")
# Faker.seed(0)

def gen_address():
    fake = Faker("pt_BR")
    def gen_complement():
        if randint(0,10) > 4:
            return "apto " + str(randint(1,22)) + "0" + str(randint(1, 6))
        return ""
    
    while True:
        try:
            addr = fake.address()
            addr_lines = addr.split("\n")
            street, number = addr_lines[0].split(",")
            postal_code = addr_lines[2].split(" ")[0]
            locality = addr_lines[1].strip()
            region_code = addr_lines[2][-2:]
            city = addr_lines[2][9:-4].strip()

            return {
                    "street": street.strip(),
                    "number": number.strip(),
                    "complement": gen_complement(),
                    "locality": locality,
                    "city": city,
                    "region_code": region_code,
                    "country": "BRA",
                    "postal_code": postal_code.strip()
                }
        except:
            ...

def gen_customer(fake = None):
    if fake is None:
        fake = Faker("pt_BR")
        
    mobile_phone = fake.msisdn()
    first_name = fake.first_name()
    last_name = fake.last_name()
    return {
        "id": "CID_" + str(uuid.uuid4()),
        "created_at": str(fake.date_between(
            start_date = '-5y', 
            end_date = 'today'
            )),
        "name": f"{first_name} {last_name}",
        "email": f"{first_name.replace(" ", "-").lower()}.{last_name.replace(" ", "-").lower()}@exemple.com",
        "tax_id": fake.cpf(),
        "address": gen_address(),
        "phones": [
            {
                "country": "+55",
                "area": mobile_phone[-11:],
                "type": "MOBILE"
            }
        ]
    }


if __name__ == "__main__":
    import json

    fake = Faker("pt_BR")
    Faker.seed(0)

    max_customers = 50
    customers = [gen_customer(fake = fake) for _ in range(max_customers)]

    with open("src/data/customers.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(customers, indent=4))

    # print(len(customers))
    # print(json.dumps(customers[randint(0, len(customers))], indent=4))
    