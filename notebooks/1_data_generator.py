# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd

# COMMAND ----------

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_ORDERS = 7000
NUM_PRODUCTS = 800
NUM_CUSTOMERS = 4000

DIRTY_RATIO = 0.07
NULL_RATIO = 0.03
DUPLICATE_RATIO = 0.0001
INCREMENTAL_RATIO = 0.10

COUNTRY = "India"
BASE_CURRENCY = "USD"
START_DATE = datetime(2022,1,1)
END_DATE = datetime(2025,6,30)
NUM_RATE_DAYS = 100

# COMMAND ----------

# MAGIC %md
# MAGIC Master Lookup

# COMMAND ----------

CATEGORY_SUBCATEGORY = {
    "Electronics":["Smartphones","Laptops","TV"],
    "Fashion":["Shirts","Shoes","Watches"],
    "Home":["Furniture","Kitchen","Decor"],
    "Groceries": ["Beverages", "Snacks", "Packaged Foods"]
}

BRANDS = {
    "Smartphones":["Apple","Samsung","OnePlus", "Xiaomi", "Motorola"],
    "Laptops":["Dell","HP","Lenovo", "iMac"],
    "TV":["Sony","LG","Samsung", "Panasonic"],
    "Shirts":["Nike","Puma", "US Polo", "Levis"],
    "Shoes":["Adidas","Nike", "Reebok","Puma"],
    "Watches":["Titan","Casio", "Rolex"],
    "Furniture":["Ikea", "Westelm"],
    "Kitchen":["Prestige", "Godrej", "Whirlpool"],
    "Decor":["HomeCentre"],
    "Beverages": ["Coca-Cola", "Pepsi", "Tata Tea"],
    "Snacks": ["Lay's", "Haldiram's", "Cadbury"],
    "Packaged Foods": ["Maggi", "Kissan", "Kellogg's"]
}

STORE_CODES = [ "CHE001", "BLR001", "MUM001", "DEL001", "HYD001", "CHE002", "BLR002", "MUM002", "DEL002", "HYD002", "CHE003", "BLR003", "MUM003", "DEL003", "HYD003" ]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com"]

INDIAN_CITIES = [ ("Chennai","Tamil Nadu"), ("Bangalore","Karnataka"), ("Mumbai","Maharashtra"), ("Hyderabad","Telangana"), ("Pune","Maharashtra"), ("New Delhi","Delhi"), ("Kolkata","West Bengal"), ("Ahmedabad","Gujarat"), ("Surat","Gujarat"), ("Jaipur","Rajasthan"), ("Lucknow","Uttar Pradesh"), ("Kanpur","Uttar Pradesh") ]

CURRENCIES = [ "EUR", "GBP", "INR", "JPY", "AUD", "SGD", "AED" ]

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Generic Helper Functions

# COMMAND ----------

# generic id generator
def generate_ids(prefix, start_num, count):
    return [ f"{prefix}{i}"
        for i in range(start_num, start_num + count) ]
def generate_ids(prefix, start_num, count, zero_pad=0 ):
    return [ f"{prefix}{str(i).zfill(zero_pad)}"
        for i in range( start_num, start_num + count ) ]
# to generate date
def random_date( start_date=START_DATE, end_date=END_DATE, date_format="%Y-%m-%d" ):
    delta_days = (end_date - start_date).days
    random_days = random.randint(0, delta_days)
    random_dt = start_date + timedelta(days=random_days)
    return random_dt.strftime(date_format)
# to generate IND phone number
def generate_phone():
    first_digit = random.choice(["9", "8"])
    remaining = "".join(random.choices("0123456789", k=9))
    return first_digit + remaining

# COMMAND ----------

def should_make_dirty(dirty_ratio=DIRTY_RATIO):
    return random.random() < dirty_ratio
def should_make_null(null_ratio=NULL_RATIO):
    return random.random() < NULL_RATIO
def should_duplicate(duplicate_ratio=DUPLICATE_RATIO):
    return random.random() < DUPLICATE_RATIO


# COMMAND ----------

product_ids=[]
def generate_products():
    products_data = []
    global product_ids
    product_ids = generate_ids(prefix="P", start_num=200, count=NUM_PRODUCTS)
    for product_id in product_ids:
        category = random.choice(list(CATEGORY_SUBCATEGORY.keys()))
        subcategory = random.choice(CATEGORY_SUBCATEGORY[category])
        brand = random.choice(BRANDS[subcategory])
        product_name = (
            f"{brand} {subcategory} "
            f"Model-{random.randint(100,999)}"
        )
        cost_price = round(random.uniform(100.0,100000.0), 2)
        if should_make_dirty():
            cost_price = random.choice([-100.0, 0.0, 99999.0 ])
        if should_make_null():
            product_name = None
        if should_make_null():
            brand = None
        if should_make_null():
            subcategory = None
        record = {
            "ProductID": product_id,
            "ProductName": product_name,
            "Category": category,
            "SubCategory": subcategory,
            "Brand": brand,
            "CostPrice": cost_price
        }
        products_data.append(record)
        # product_ids.append(product_id)

    return products_data

customer_ids = []
def generate_customers():
    customers_data = []
    global customer_ids
    customer_ids = list(range(1000, 1000 + NUM_CUSTOMERS))
    for customer_id in customer_ids:
        first_name = fake.first_name()
        last_name = fake.last_name()
        if should_make_dirty():
            choice = random.choice(["space", "lower"])
            if choice == "space":
                first_name = "   " + first_name
                last_name = "    " + last_name
            elif choice == "lower":
                first_name = first_name.lower()
                last_name = last_name.lower()
        domain = random.choice(EMAIL_DOMAINS)
        email = (
            f"{first_name.strip().lower()}."
            f"{last_name.strip().lower()}@{domain}"
        )
        phone = generate_phone()
        city, state = random.choice(INDIAN_CITIES)
        last_updated = ( datetime.now() - timedelta( days=random.randint(0,365) ) )
        if should_make_dirty():
            email = random.choice([ "wrong_email", "abcgmail.com", " ", None ])
        if should_make_dirty():
            phone = random.choice([ "123", "abcdef", " ", None ])
        if should_make_null():
            city = None
        if should_make_null():
            state = None
        record = {
            "CustomerID": customer_id,
            "FirstName": first_name,
            "LastName": last_name,
            "Email": email,
            "Phone": phone,
            "City": city,
            "State": state,
            "LastUpdated": last_updated.strftime("%Y-%m-%d %H:%M:%S")
        }
        customers_data.append(record)

    return customers_data

def generate_orders():  
    orders_data = []
    order_ids = generate_ids(prefix="ORD", start_num=1, count=7000, zero_pad=4 )
    for order_id in order_ids:
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        order_date = random_date()
        quantity = random.randint(1,10)
        unit_price = round( random.uniform(100.0, 100000.0 ), 2 )
        store_code = random.choice( STORE_CODES )
        if should_make_dirty():
            quantity = random.choice( [-5, 0, 1000] )
        if should_make_dirty():
            unit_price = random.choice([-100.0, 0.0, 99999.0 ])
        if should_make_null():
            store_code = None
        if should_make_null():
            quantity = None
        if should_make_null():
            unit_price = None
        record = {
            "OrderID": order_id,
            "CustomerID": customer_id,
            "ProductID": product_id,
            "OrderDate": order_date,
            "Quantity": quantity,
            "UnitPrice": unit_price,
            "StoreCode": store_code
        }
        orders_data.append(record)
        if should_duplicate():
            orders_data.append(record.copy())
    return orders_data

def generate_exchange_rates():
    exchange_data = []
    for i in range(NUM_RATE_DAYS):
        rate_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        rates = []
        for target_currency in CURRENCIES:
            exchange_rate = round(random.uniform(0.5, 100.0), 6)
            if should_make_dirty():
                exchange_rate = random.choice( [-1.0, 0.0, 999.9, -0.9, -83.25] )
            currency = target_currency
            if should_make_null():
                currency = None
            rates.append(
                {
                    "TargetCurrency": currency,
                    "ExchangeRate": exchange_rate
                }
            )
        record = {
            "BaseCurrency": BASE_CURRENCY,
            "RateDate": rate_date,
            "Rates": rates
        }
        exchange_data.append(record)
    return exchange_data


# COMMAND ----------

def should_make_incremental():

    return random.random() < INCREMENTAL_RATIO

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

def save_dataset(data, path, format="csv"):
    pdf = pd.DataFrame(data)
    for col in ["CostPrice", "UnitPrice", "ExchangeRate"]:
        if col in pdf.columns:
            pdf[col] = pdf[col].astype(float)
    df = spark.createDataFrame(pdf)
    if format == "csv":
        df.write.mode("overwrite").option("header", True).csv(path)
    else:
        df.write.mode("overwrite").json(path)

# COMMAND ----------

products_data = generate_products()
customers_data = generate_customers()
orders_data = generate_orders()
exchange_data = generate_exchange_rates()

# COMMAND ----------

config = load_config()

for source in config["sources"]:

    if source["source_name"] == "products":
        data = generate_products()

    elif source["source_name"] == "customers":
        data = generate_customers()

    elif source["source_name"] == "orders":
        data = generate_orders()

    elif source["source_name"] == "exchange_rates":
        data = generate_exchange_rates()

    save_dataset(
        data,
        get_source_path(source, config),
        source["format"]
    )

# COMMAND ----------

exchange_data = generate_exchange_rates()
save_dataset(exchange_data, "/Volumes/retail/bronze/raw_files/exchange_rates", "json")

# COMMAND ----------

# TO VERIFY
display(spark.read.option("header",True).csv("/Volumes/retail/bronze/raw_files/orders"))


# COMMAND ----------

display(spark.read.option("header",True).csv("/Volumes/retail/bronze/raw_files/customers"))

# COMMAND ----------

display(spark.read.option("header",True).csv("/Volumes/retail/bronze/raw_files/products"))


# COMMAND ----------


display(spark.read.json("/Volumes/retail/bronze/raw_files/exchange_rates"))