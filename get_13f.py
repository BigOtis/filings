from edgar import *
import os
import json
import xmltodict
from pymongo import MongoClient
from pprint import pprint
from datetime import datetime

# Set up your EDGAR identity in your environment
os.environ['EDGAR_IDENTITY'] = "Your Name your.email@example.com"  

def store_in_mongo(cik, name, data):
    mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")
    client = MongoClient(mongo_url)
    db = client.sec_filings
    collection = db.filings_13f
    # Ensure filing_date is a datetime object
    data['filing_date'] = datetime.strptime(data['filing_date'], '%Y-%m-%d')
    data['cik'] = cik
    data['name'] = name
    result = collection.update_one(
        {"cik": cik, "accession_number": data['accession_number']},
        {"$set": data},
        upsert=True
    )
    print(f"Data stored for CIK {cik} ({name}): {data['accession_number']}")

def pretty_print_data(data):
    # Use pprint to enhance the readability of the data
    print("Formatted 13F Filing Data:")
    pprint(data)

def convert_xml_to_json(xml_data):
    # Convert the XML data to JSON
    json_data = xmltodict.parse(xml_data)
    return json_data

def fetch_and_store_13f_filings(cik, name):
    # Fetch filings using the Company object and specifying the form type
    company = Company(cik)
    filing = company.get_filings(form="13F-HR").latest(1)
    print(filing)
    thirteenf = filing.obj()
    json_data = convert_xml_to_json(thirteenf.infotable_xml)
    json_data['accession_number'] = filing.accession_number
    json_data['filing_date'] = str(filing.filing_date)  # Convert date to string
    store_in_mongo(cik, name, json_data)

def retrieve_13f_data(cik, start_date, end_date):
    mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")
    client = MongoClient(mongo_url)
    db = client.sec_filings
    collection = db.filings_13f
    
    query = {
        "cik": cik,
        "filing_date": {
            "$gte": datetime.strptime(start_date, '%Y-%m-%d'),
            "$lte": datetime.strptime(end_date, '%Y-%m-%d')
        }
    }
    
    results = collection.find(query)
    return list(results)

def generate_statistics(filings):
    total_value = 0
    total_shares = 0
    top_holdings_value = []
    top_holdings_shares = []
    discretion_count = {}
    issuer_value = {}
    unique_issuers = set()

    for filing in filings:
        infotable = filing['informationTable']['infoTable']
        for item in infotable:
            value = int(item['value'])
            shares = int(item['shrsOrPrnAmt']['sshPrnamt'])
            total_value += value
            total_shares += shares

            # Track top holdings by value
            top_holdings_value.append((item['nameOfIssuer'], value))

            # Track top holdings by shares
            top_holdings_shares.append((item['nameOfIssuer'], shares))

            # Count investment discretion
            discretion = item.get('shrsOrPrnAmt', {}).get('investmentDiscretion', 'Unknown')
            if discretion in discretion_count:
                discretion_count[discretion] += 1
            else:
                discretion_count[discretion] = 1

            # Sum value by issuer
            issuer = item['nameOfIssuer']
            if issuer in issuer_value:
                issuer_value[issuer] += value
            else:
                issuer_value[issuer] = value

            # Track unique issuers
            unique_issuers.add(issuer)

    top_holdings_value = sorted(top_holdings_value, key=lambda x: x[1], reverse=True)[:5]
    top_holdings_shares = sorted(top_holdings_shares, key=lambda x: x[1], reverse=True)[:5]
    average_value_per_holding = total_value / len(unique_issuers) if unique_issuers else 0

    print(f"Total Value of Holdings: ${total_value:,}")
    print(f"Total Number of Shares: {total_shares:,}")
    print(f"Average Value per Holding: ${average_value_per_holding:,}")
    print(f"Total Number of Unique Issuers: {len(unique_issuers)}")

    print("\nTop 5 Holdings by Value:")
    for issuer, value in top_holdings_value:
        print(f"{issuer}: ${value:,}")

    print("\nTop 5 Holdings by Shares:")
    for issuer, shares in top_holdings_shares:
        print(f"{issuer}: {shares:,} shares")

    print("\nDistribution by Investment Discretion:")
    for discretion, count in discretion_count.items():
        print(f"{discretion}: {count} holdings")

    print("\nTop 5 Holdings by Issuer Value:")
    for issuer, value in sorted(issuer_value.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"{issuer}: ${value:,}")

def main():
    # Example CIKs
    ciks = {
        '1067983': 'Warren Buffett',
        '1166559': 'Gates Foundation',
        '102909': "Vangaurd Group Inc",
        '0001364742': 'BlackRock',
        '0000093751': 'State Street',
        '0000895421': 'Morgan Stanley',
        '00019617': 'JPMorgan Chase & Co.',
        '0001423053': 'Geode Capital Management',
        '0000070858': 'Bank of America',
    }
    for cik, name in ciks.items():
        print(f"Starting retrieval for {name}")
        fetch_and_store_13f_filings(cik, name)

    # Example of retrieving data
    start_date = '2020-01-01'
    end_date = '2024-06-01'
    investor_cik = '1067983'
    filings = retrieve_13f_data(investor_cik, start_date, end_date)
    pretty_print_data(filings)
    generate_statistics(filings)

if __name__ == "__main__":
    main()
