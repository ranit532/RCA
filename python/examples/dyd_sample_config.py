"""
Sample DYD Integration Configuration
Demonstrates how to integrate with Discover Your Data tool outputs
"""

# Sample DYD Mappings JSON (export from DYD tool)
DYD_SAMPLE_MAPPINGS = {
    "mappings": [
        {
            "source_system": "LEGACY_ASSET_SYSTEM",
            "source_entity": "ASSET_PARTY",
            "source_columns": ["PARTY_ID", "PARTY_CODE", "PARTY_NAME", "PARTY_TYPE"],
            "target_table": "RAW.DIM_PARTY_RAW",
            "target_columns": ["PARTY_ID", "PARTY_CODE", "PARTY_NAME", "PARTY_TYPE"],
            "confidence": 95.0,
            "transformation": "SELECT * FROM LEGACY_ASSET_SYSTEM.PARTY",
            "join_keys": ["PARTY_ID"]
        },
        {
            "source_system": "LEGACY_ASSET_SYSTEM",
            "source_entity": "ASSET_ACCOUNT",
            "source_columns": ["ACCOUNT_ID", "ACCOUNT_CODE", "PARTY_ID", "ACCOUNT_TYPE"],
            "target_table": "RAW.DIM_ACCOUNT_RAW",
            "target_columns": ["ACCOUNT_ID", "ACCOUNT_CODE", "PARTY_ID", "ACCOUNT_TYPE"],
            "confidence": 92.0,
            "transformation": "SELECT * FROM LEGACY_ASSET_SYSTEM.ACCOUNT",
            "join_keys": ["ACCOUNT_ID"]
        },
        {
            "source_system": "MARKET_DATA_SYSTEM",
            "source_entity": "INSTRUMENT_MASTER",
            "source_columns": ["INSTRUMENT_ID", "ISIN", "RIC", "ASSET_CLASS", "REFERENCE_PRICE"],
            "target_table": "RAW.DIM_INSTRUMENT_RAW",
            "target_columns": ["INSTRUMENT_ID", "ISIN", "RIC", "ASSET_CLASS", "REFERENCE_PRICE"],
            "confidence": 98.0,
            "transformation": "SELECT * FROM MARKET_DATA_SYSTEM.INSTRUMENTS",
            "join_keys": ["INSTRUMENT_ID"]
        }
    ]
}

# Sample DYD Metadata JSON (export from DYD tool)
DYD_SAMPLE_METADATA = {
    "metadata": [
        {
            "entity_name": "DIM_PARTY_RAW",
            "entity_type": "TABLE",
            "description": "Party master data from legacy system"
        },
        {
            "entity_name": "DIM_PARTY_RAW",
            "entity_type": "FIELD",
            "column_name": "PARTY_ID",
            "data_type": "NUMBER",
            "description": "Unique party identifier",
            "business_term": "PartyID",
            "sample_values": ["1", "2", "3"]
        },
        {
            "entity_name": "DIM_PARTY_RAW",
            "entity_type": "FIELD",
            "column_name": "PARTY_CODE",
            "data_type": "VARCHAR",
            "description": "External party code",
            "business_term": "PartyCode",
            "sample_values": ["CLIENT_00000001", "CLIENT_00000002"]
        },
        {
            "entity_name": "DIM_PARTY_RAW",
            "entity_type": "FIELD",
            "column_name": "PARTY_TYPE",
            "data_type": "VARCHAR",
            "description": "Classification of party",
            "business_term": "PartyType",
            "sample_values": ["INSTITUTIONAL", "RETAIL"]
        },
        {
            "entity_name": "FCT_TRADE_RAW",
            "entity_type": "TABLE",
            "description": "Trade transactions from legacy system"
        },
        {
            "entity_name": "FCT_TRADE_RAW",
            "entity_type": "FIELD",
            "column_name": "TRADE_ID",
            "data_type": "NUMBER",
            "description": "Unique trade identifier",
            "business_term": "TradeID",
            "sample_values": ["1000001", "1000002"]
        },
        {
            "entity_name": "FCT_TRADE_RAW",
            "entity_type": "FIELD",
            "column_name": "GROSS_AMOUNT",
            "data_type": "NUMBER",
            "description": "Trade notional amount",
            "business_term": "TradeAmount",
            "sample_values": ["1000000.00", "500000.00"]
        }
    ]
}

# Example usage
if __name__ == "__main__":
    import json
    
    # Save sample mappings to file
    with open("dyd_mappings_sample.json", "w") as f:
        json.dump(DYD_SAMPLE_MAPPINGS, f, indent=2)
    
    # Save sample metadata to file
    with open("dyd_metadata_sample.json", "w") as f:
        json.dump(DYD_SAMPLE_METADATA, f, indent=2)
    
    print("✅ Sample DYD configurations created")
    print("  - dyd_mappings_sample.json")
    print("  - dyd_metadata_sample.json")
