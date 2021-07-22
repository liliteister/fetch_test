#!/usr/bin/env python
# coding: utf-8

# ## Fetch Rewards Coding Exercise
# 7/19/2021


import json
import pandas as pd
import gzip
from datetime import datetime
import sqlite3
import urllib
import os


def download_file(link, local_path):
    if os.path.exists(local_path):
        print(f"{local_path} exists.")
    else:
        urllib.request.urlretrieve(link, local_path)
        print(f"{local_path} downloaded.")
        
r = "https://fetch-hiring.s3.amazonaws.com/data-analyst/ineeddata-data-modeling/receipts.json.gz"
u = "https://fetch-hiring.s3.amazonaws.com/data-analyst/ineeddata-data-modeling/users.json.gz"
b = "https://fetch-hiring.s3.amazonaws.com/data-analyst/ineeddata-data-modeling/brands.json.gz"

download_file(r, "receipts.json.gz")
download_file(u, "users.json.gz")
download_file(b, "brands.json.gz")


# Receipts data

# Load json into pandas dataframe(s)

rcpt_file = "receipts.json.gz" 
rdata = []

with gzip.open(rcpt_file, "rb") as f:
    for line in f:
        rdata.append(json.loads(line))

receipts = pd.json_normalize(rdata)
receipts.rename(columns={"_id.$oid":"id","createDate.$date":"createDate","dateScanned.$date":"dateScanned",
                         "finishedDate.$date":"finishedDate","modifyDate.$date":"modifyDate",
                        "pointsAwardedDate.$date":"pointsAwardedDate","purchaseDate.$date":"purchaseDate"},
               inplace=True)

item_nn = receipts[["id","rewardsReceiptItemList"]].dropna(subset=["rewardsReceiptItemList"])

itemvals = []
for index, row in item_nn.iterrows():
    idx = row["id"]
    for item in row["rewardsReceiptItemList"]:
        d = dict(item)
        d['receiptId'] = idx
        
        itemvals.append(d)
        
items = pd.json_normalize(itemvals)
items['generatedId'] = items.index


def convert_date(col):
    if pd.isna(col):
        return None
    else:
        return datetime.fromtimestamp(col/1000).strftime('%Y-%m-%d %H:%M:%S')

receipts['createDate'] = receipts['createDate'].apply(convert_date)
receipts['dateScanned'] = receipts['dateScanned'].apply(convert_date)
receipts['finishedDate'] = receipts['finishedDate'].apply(convert_date)
receipts['modifyDate'] = receipts['modifyDate'].apply(convert_date)
receipts['pointsAwardedDate'] = receipts['pointsAwardedDate'].apply(convert_date)
receipts['purchaseDate'] = receipts['purchaseDate'].apply(convert_date)
receipts.drop(columns=['rewardsReceiptItemList'], inplace=True)


print("Receipts:")
print(f"Records: {len(receipts)}")
print(f"Deduplicated Records: {len(receipts.drop_duplicates())}")
print(f"Deduplicated userIds: {len(receipts['id'].drop_duplicates())}")


# Load dataframes into SQL tables.

def sqlite_connection(dbname):
    c = None
    try:
        c = sqlite3.connect(dbname)
        print(f"Database {dbname} created with sqlite version {sqlite3.version}")
    except Error as err:
        print(err)
        
    return c

conn = sqlite_connection('fetchTest.db')

c = conn.cursor()

tbl_receipts = """
CREATE TABLE IF NOT EXISTS receiptsFact (
    id VARCHAR(100) PRIMARY KEY,
    userId VARCHAR(100),
    createDate DATETIME,
    dateScanned DATETIME,
    finishedDate DATETIME,
    modifyDate DATETIME,
    pointsAwardedDate DATETIME,
    purchaseDate DATETIME,
    bonusPointsEarned INT,
    bonusPointsEarnedReason VARCHAR(8000),
    pointsEarned INT,
    purchasedItemCount INT,
    rewardsReceiptStatus VARCHAR(100),
    totalSpent NUMERIC
)
"""

tbl_items = """
CREATE TABLE IF NOT EXISTS receiptItemsDim (
    generatedId INT PRIMARY KEY,
    receiptId VARCHAR(100),
    barcode VARCHAR(100), 
    description VARCHAR(8000), 
    finalPrice NUMERIC, 
    itemPrice NUMERIC, 
    needsFetchReview BIT,
    partnerItemId VARCHAR(100), 
    preventTargetGapPoints BIT, 
    quantityPurchased INT,
    userFlaggedBarcode VARCHAR(100), 
    userFlaggedNewItem BIT, 
    userFlaggedPrice NUMERIC,
    userFlaggedQuantity INT, 
    needsFetchReviewReason VARCHAR(8000),
    pointsNotAwardedReason VARCHAR(8000), 
    pointsPayerId VARCHAR(100), 
    rewardsGroup VARCHAR(200),
    rewardsProductPartnerId VARCHAR(100), 
    userFlaggedDescription VARCHAR(8000),
    originalMetaBriteBarcode VARCHAR(100), 
    originalMetaBriteDescription VARCHAR(8000), 
    brandCode VARCHAR(200),
    competitorRewardsGroup VARCHAR(200), 
    discountedItemPrice NUMERIC,
    originalReceiptItemText VARCHAR(1000), 
    itemNumber INT,
    originalMetaBriteQuantityPurchased INT, 
    pointsEarned NUMERIC, 
    targetPrice NUMERIC,
    competitiveProduct BIT, 
    originalFinalPrice NUMERIC,
    originalMetaBriteItemPrice NUMERIC, 
    deleted BIT, 
    priceAfterCoupon NUMERIC,
    metabriteCampaignId VARCHAR(1000)
)
"""

c.execute('''DROP TABLE IF EXISTS receiptsFact''')
c.execute('''DROP TABLE IF EXISTS receiptItemsDim''')
c.execute(tbl_receipts)
c.execute(tbl_items)
receipts.to_sql("receiptsFact", conn, if_exists='append', index=False)
items.to_sql("receiptItemsDim", conn, if_exists='append', index=False)
conn.commit()

conn.close()


# User data
# Import json data into pandas dataframe.

user_file = "users.json.gz" 
udata = []

with gzip.open(user_file, "rb") as f:
    for line in f:
        udata.append(json.loads(line))

users = pd.json_normalize(udata)
users.rename(columns={"_id.$oid":"userId","createdDate.$date":"createdDate","lastLogin.$date":"lastLoginDate"}, inplace=True)
users['createdDate'] = users['createdDate'].apply(convert_date)
users['lastLoginDate'] = users['lastLoginDate'].apply(convert_date)

# There are duplicate records in the user table. It appears that they are true duplicates in which all columns are identical. Therefore, duplicates will be simply dropped.

print(f"Records: {len(users)}")
print(f"Deduplicated Records: {len(users.drop_duplicates())}")
print(f"Deduplicated userIds: {len(users['userId'].drop_duplicates())}")

users.drop_duplicates(inplace=True)
users.head()


# Load data to sql tables

conn = sqlite_connection('fetchTest.db')
c = conn.cursor()

tbl_users = """
CREATE TABLE IF NOT EXISTS usersDim (
    userId VARCHAR(100) PRIMARY KEY,
    role VARCHAR(100),
    signUpSource VARCHAR(100),
    state VARCHAR(2),
    createdDate DATETIME,
    lastLoginDate DATETIME,
    active BIT
)
"""

c.execute('''DROP TABLE IF EXISTS usersDim''')
c.execute(tbl_users)
users.to_sql("usersDim", conn, if_exists='append', index=False)
conn.commit()

conn.close()

# Brands data
# Read json data into pandas dataframe.

brnd_file = "brands.json.gz" 
bdata = []

with gzip.open(brnd_file, "rb") as f:
    for line in f:
        bdata.append(json.loads(line))

brands = pd.json_normalize(bdata)
brands.rename(columns={"_id.$oid":"brandId","cpg.$id.$oid":"cpgId","cpg.$ref":"cpg"}, inplace=True)


# There are no duplicated records on brandId, but the barcodes field is not unique.

print(f"Records: {len(brands)}")
print(f"Deduplicated Records: {len(brands.drop_duplicates())}")
print(f"Deduplicated brandIds: {len(brands['brandId'].drop_duplicates())}")
print(f"Deduplicated barcodes: {len(brands['barcode'].drop_duplicates())}")


# Load to SQL tables. The primary key will be brandId.

conn = sqlite_connection('fetchTest.db')
c = conn.cursor()

tbl_brands = """
CREATE TABLE IF NOT EXISTS brandDim (
    brandId VARCHAR(100) PRIMARY KEY,
    barcode VARCHAR(100),
    category VARCHAR(200),
    categoryCode VARCHAR(200),
    name VARCHAR(1000),
    topBrand BIT,
    cpgId VARCHAR(100),
    cpg VARCHAR(60),
    brandCode VARCHAR(100)
)
"""

c.execute('''DROP TABLE IF EXISTS brandsDim''')
c.execute(tbl_brands)
brands.to_sql("brandsDim", conn, if_exists='append', index=False)
conn.commit()

conn.close()





