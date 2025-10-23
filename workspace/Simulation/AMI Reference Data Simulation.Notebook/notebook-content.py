# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "374bb522-cbc7-4a32-9b1f-837408d5aa3f",
# META       "default_lakehouse_name": "ReferenceDataLH",
# META       "default_lakehouse_workspace_id": "9db5ef38-822d-490e-a9d9-c7a8de1dc223",
# META       "known_lakehouses": [
# META         {
# META           "id": "374bb522-cbc7-4a32-9b1f-837408d5aa3f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # AMI Smart Meter Reference Data Simulation
# 
# This notebook generates simulated reference data for an Advanced Metering Infrastructure (AMI) system. It generates realistic reference data (substations, feeder lines, transformers and meters and stores the results into a Microsoft Fabric Lakehouse)
# 
# ## Features:
# - Realistic meter reference data with specifications and network topology
# - Stores results as Lakehouse tables
# 
# ## Requirements:
# - PySpark
# - Faker library for realistic data generation

# MARKDOWN ********************

# ## 1. Setup PySpark and Import Libraries
# 
# Initialize PySpark session and import all required libraries for AMI telemetry simulation.

# CELL ********************

raise Exception("Simulated reference data has already been generated. DO NOT re-run this notebook to avoid referential integrity issues with historical data!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Install required packages if not already installed
!pip install faker pandas numpy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np

# Faker for realistic data generation
from faker import Faker
from faker.providers import automotive, internet, phone_number

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Azure Event Hub imports
try:
    from azure.eventhub import EventHubProducerClient, EventData
    AZURE_AVAILABLE = True
except ImportError:
    print("Azure Event Hub SDK not available. Install with: pip install azure-eventhub")
    AZURE_AVAILABLE = False

# Initialize Faker
fake = Faker()
fake.add_provider(automotive)
fake.add_provider(internet)
fake.add_provider(phone_number)

# Set random seed for reproducible results
random.seed(42)
np.random.seed(42)
Faker.seed(42)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AMI_Telemetry_Simulation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

print(f"Spark version: {spark.version}")
print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
print("PySpark session initialized successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define base geographic coordinates
# City center
CITY_CENTER = {
    'lat': 39.76838,
    'lon': -86.15804
}

CITY_BOUNDS_NORTH = CITY_CENTER['lat'] + .20
CITY_BOUNDS_SOUTH = CITY_CENTER['lat'] - 0.20
CITY_BOUNDS_EAST = CITY_CENTER['lon'] + .15
CITY_BOUNDS_WEST = CITY_CENTER['lon'] - 0.15

# City boundaries (approximate)
CITY_BOUNDS = {
    'north': CITY_BOUNDS_NORTH,   # Northern boundary
    'south': CITY_BOUNDS_SOUTH,   # Southern boundary  
    'east': CITY_BOUNDS_EAST,   # Eastern boundary
    'west': CITY_BOUNDS_WEST    # Western boundary
}

# Calculate city dimensions
CITY_WIDTH = CITY_BOUNDS['east'] - CITY_BOUNDS['west']  
CITY_HEIGHT = CITY_BOUNDS['north'] - CITY_BOUNDS['south'] 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Create Feeder Line and Network Topology Data
# 
# Generate realistic electrical grid topology with feeder lines, transformers, and meter assignments to simulate real AMI network structure.

# CELL ********************

def generate_network_topology():
    """Generate realistic electrical grid topology"""
    
    # Generate substations
    substations = []
    substation_names = []
    substation_count = 15
        
    for i in range(substation_count):
        substation_names.append(fake.city() + " Sub")

    for i, name in enumerate(substation_names):
        substation = {
            "substation_id": f"SUB_{str(i+1).zfill(3)}",
            "substation_name": name,
            "voltage_level": "12.47kV",
            "capacity_mva": random.randint(25, 100),
            "latitude": CITY_CENTER['lat'] + np.random.normal(-0.09, 0.09),
            "longitude": CITY_CENTER['lon'] + np.random.normal(-0.12, 0.12),
            "commissioned_date": fake.date_between(start_date='-20y', end_date='-5y').isoformat()
        }
        substations.append(substation)
    
    # Generate feeder lines
    feeders = []
    feeder_count_per_sub = 4
    
    counter = 0
    for sub in substations:
        for i in range(feeder_count_per_sub  + random.randint(0, 3)):
            substation_id = sub["substation_id"]
            feeder = {
                "feeder_id": f"FDR_{str(counter+1).zfill(3)}",
                "feeder_name": f"Feeder_{chr(65 + (i % 26))}",
                "substation_id": substation_id,
                "voltage_level": "12.47kV",
                "max_load_kw": random.randint(3000, 8000),
                "length_miles": random.uniform(2.5, 12.0),
                "conductor_type": random.choice(["ACSR", "Aluminum", "Copper"]),
                "protection_scheme": random.choice(["OCR", "Distance", "Differential"]),
                "scada_monitored": random.choice([True, False]),
                "latitude": sub["latitude"],
                "longitude": sub["longitude"]
            }
            feeders.append(feeder)
            counter = counter + 1
    
    # Generate transformers
    transformers = []
    transformer_count_per_feeder = 4 # Multiple transformers per feeder
    
    counter = 0
    for feeder in feeders:
        for i in range(transformer_count_per_feeder + random.randint(0, 3)):
            feeder_id = feeder["feeder_id"]
            transformer = {
                "transformer_id": f"XFMR_{str(counter+1).zfill(4)}",
                "feeder_id": feeder_id,
                "transformer_type": random.choice(["Pad-mounted", "Pole-mounted", "Underground", "Vault"]),
                "primary_voltage": 12470,
                "secondary_voltage": random.choice([120, 240, 480]),
                "kva_rating": random.choice([25, 50, 75, 100, 167, 250, 500]),
                "phase_config": random.choice(["Single", "Three"]),
                "install_date": fake.date_between(start_date='-15y', end_date='today').isoformat(),
                "manufacturer": random.choice(["ABB", "Schneider", "Eaton", "General Electric", "Cooper"]),
                "latitude": feeder["latitude"] + np.random.normal(-0.01, 0.01),
                "longitude": feeder["longitude"] + np.random.normal(-0.01, 0.01),
                "load_tap_changer": random.choice([True, False]),
                "temperature_monitoring": random.choice([True, False])
            }
            transformers.append(transformer)
            counter = counter + 1
    

    # Generate realistic reference data for smart meters
    meters = []
    meter_count_per_transformer = 10

    METER_MODELS = [
        {"manufacturer": "Itron", "model": "Centron C2SOD", "type": "residential", "max_amps": 200, "voltage": 240, "phases": 1},
        {"manufacturer": "Landis+Gyr", "model": "E650", "type": "residential", "max_amps": 200, "voltage": 240, "phases": 1},
        {"manufacturer": "Sensus", "model": "iCon A", "type": "residential", "max_amps": 320, "voltage": 240, "phases": 1},
        {"manufacturer": "Itron", "model": "Centron C1S", "type": "commercial", "max_amps": 400, "voltage": 480, "phases": 3},
        {"manufacturer": "Landis+Gyr", "model": "E850", "type": "commercial", "max_amps": 400, "voltage": 480, "phases": 3},
        {"manufacturer": "GE", "model": "I-210+c", "type": "residential", "max_amps": 200, "voltage": 240, "phases": 1},
        {"manufacturer": "Schneider Electric", "model": "ION7550", "type": "industrial", "max_amps": 800, "voltage": 480, "phases": 3},
        {"manufacturer": "Honeywell", "model": "Eagle", "type": "residential", "max_amps": 200, "voltage": 240, "phases": 1}
        ]

    counter = 0
    for transformer in transformers:
        for i in range(meter_count_per_transformer + random.randint(0, 15)):
            # Select meter model based on realistic distribution
            meter_type_prob = random.random()
            if meter_type_prob < 0.75:  # 75% residential
                available_models = [m for m in METER_MODELS if m["type"] == "residential"]
            elif meter_type_prob < 0.95:  # 20% commercial
                available_models = [m for m in METER_MODELS if m["type"] == "commercial"]
            else:  # 5% industrial
                available_models = [m for m in METER_MODELS if m["type"] == "industrial"]
            
            model_spec = random.choice(available_models)
            
            # Generate installation date (last 10 years, with most recent in last 3 years)
            install_date_prob = random.random()
            if install_date_prob < 0.6:  # 60% installed in last 3 years
                install_date = fake.date_between(start_date='-3y', end_date='today')
            else:  # 40% installed 3-10 years ago
                install_date = fake.date_between(start_date='-10y', end_date='-3y')
            
            # Generate coordinates for realistic geographic distribution
            # Simulate a metropolitan area centered around a major city
            base_lat, base_lon = transformer["latitude"], transformer["longitude"]
            lat_offset = np.random.normal(-0.08, 0.08)  
            lon_offset = np.random.normal(-.08, 0.08) 
            
            meter = {
                "meter_id": f"MTR{str(counter+1).zfill(6)}",
                "serial_number": fake.uuid4()[:8].upper(),
                "manufacturer": model_spec["manufacturer"],
                "model": model_spec["model"],
                "meter_type": model_spec["type"],
                "max_amps": model_spec["max_amps"],
                "voltage": model_spec["voltage"],
                "phases": model_spec["phases"],
                "install_date": install_date.isoformat(),
                "firmware_version": f"{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,99)}",
                "communication_type": random.choice(["RF Mesh", "Cellular", "PLC", "Fiber"]),
                
                # Location information
                "street_address": fake.street_address(),
                "city": fake.city(),
                "state": "IN",
                "zip_code": str(random.randint(46201, 46247)),
                "latitude": base_lat + lat_offset,
                "longitude": base_lon + lon_offset,
                
                # Customer information
                "customer_id": f"CUST{str(random.randint(100000, 999999))}",
                "customer_type": model_spec["type"],
                "service_class": random.choice(["Residential", "Commercial", "Industrial"]) if model_spec["type"] != "residential" else "Residential",
                
                # Technical specifications
                "accuracy_class": random.choice(["0.2%", "0.5%", "1.0%"]),
                "register_type": random.choice(["Digital", "LCD", "LED"]),
                "pulse_output": random.choice([True, False]),
                "tamper_detection": True,
                "power_quality_monitoring": random.choice([True, False]),
                "load_profile_interval": random.choice([5, 15, 30, 60]),  # minutes
                
                # Status
                "status": "active",
                "last_maintenance": fake.date_between(start_date=install_date, end_date='today').isoformat(),

                # Transformer alignment
                "transformer_id": transformer["transformer_id"]
            }
            meters.append(meter)
            counter = counter + 1

    # Create DataFrames
    substation_schema = StructType([
        StructField("substation_id", StringType(), False),
        StructField("substation_name", StringType(), False),
        StructField("voltage_level", StringType(), False),
        StructField("capacity_mva", IntegerType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("commissioned_date", StringType(), False)
    ])
    
    feeder_schema = StructType([
        StructField("feeder_id", StringType(), False),
        StructField("feeder_name", StringType(), False),
        StructField("substation_id", StringType(), False),
        StructField("voltage_level", StringType(), False),
        StructField("max_load_kw", IntegerType(), False),
        StructField("length_miles", DoubleType(), False),
        StructField("conductor_type", StringType(), False),
        StructField("protection_scheme", StringType(), False),
        StructField("scada_monitored", BooleanType(), False)
    ])
    
    transformer_schema = StructType([
        StructField("transformer_id", StringType(), False),
        StructField("feeder_id", StringType(), False),
        StructField("transformer_type", StringType(), False),
        StructField("primary_voltage", IntegerType(), False),
        StructField("secondary_voltage", IntegerType(), False),
        StructField("kva_rating", IntegerType(), False),
        StructField("phase_config", StringType(), False),
        StructField("install_date", StringType(), False),
        StructField("manufacturer", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("load_tap_changer", BooleanType(), False),
        StructField("temperature_monitoring", BooleanType(), False)
    ])

    meter_schema = StructType([
        StructField("meter_id", StringType(), False),
        StructField("serial_number", StringType(), False),
        StructField("manufacturer", StringType(), False),
        StructField("model", StringType(), False),
        StructField("meter_type", StringType(), False),
        StructField("max_amps", IntegerType(), False),
        StructField("voltage", IntegerType(), False),
        StructField("phases", IntegerType(), False),
        StructField("install_date", StringType(), False),
        StructField("firmware_version", StringType(), False),
        StructField("communication_type", StringType(), False),
        StructField("street_address", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("zip_code", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_type", StringType(), False),
        StructField("service_class", StringType(), False),
        StructField("accuracy_class", StringType(), False),
        StructField("register_type", StringType(), False),
        StructField("pulse_output", BooleanType(), False),
        StructField("tamper_detection", BooleanType(), False),
        StructField("power_quality_monitoring", BooleanType(), False),
        StructField("load_profile_interval", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("last_maintenance", StringType(), False),
        StructField("transformer_id", StringType(), False),
    ])
    
    substations_df = spark.createDataFrame(substations, schema=substation_schema)
    feeders_df = spark.createDataFrame(feeders, schema=feeder_schema)
    transformers_df = spark.createDataFrame(transformers, schema=transformer_schema)
    meters_df = spark.createDataFrame(meters, schema=meter_schema)
    
    return substations_df, feeders_df, transformers_df, meters_df

# Generate network topology
print("Generating network topology...")
substations_df, feeders_df, transformers_df, meters_df = generate_network_topology()

print(f"Generated {substations_df.count()} substations")
print(f"Generated {feeders_df.count()} feeders")
print(f"Generated {transformers_df.count()} transformers")
print(f"Generated {meters_df.count()} meters")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Show sample records for each entity
print("\nSubstations:")
substations_df.show(5)

print("\nSample feeders:")
feeders_df.show(5)

print("\nSample transformers:")
transformers_df.show(5)

print("\nMeters:")
meters_df.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Save results as Lakehouse Tables

# CELL ********************

#Save results as Lakehouse tables
substations_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("substations")
feeders_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("feeders")
transformers_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("transformers")
meters_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("meters")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
