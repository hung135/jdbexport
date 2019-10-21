import csv
from faker import Faker
import tqdm
import datetime

def datagenerate(records, headers):
    fake = Faker('en_US')
    fake1 = Faker('en_GB')   # To generate phone numbers
    with open("People_data1.csv", 'wt') as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        for i in tqdm.tqdm(range(records)):
            full_name = fake.name()
            FLname = full_name.split(" ")
            Fname = FLname[0]
            Lname = FLname[1]
            domain_name = "@testDomain.com"
            userId = Fname +"."+ Lname + domain_name
            
            writer.writerow({
                    "record_id" : i,
                    "email_id" : userId,
                    "prefix" : fake.prefix(),
                    "name": fake.name(),
                    "city" : fake.city(),
                    "state" : fake.state(),
                    "country" : fake.country()
                    })
    
if __name__ == '__main__':
    records = 50000
    headers = ["record_id", "email_Id", "prefix", "name", "city", "state", "country"]
    datagenerate(records, headers)
    print("CSV generation complete!")