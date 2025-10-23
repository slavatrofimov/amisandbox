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

# # AMI Smart Meter Telemetry Simulation
# 
# This notebook simulates telemetry data from thousands of smart electricity meters in an Advanced Metering Infrastructure (AMI) system. It uses realistic reference data and generates realistic telemetry payloads, failure scenarios, and sends data to Azure Event Hub.
# 
# ## Features:
# - Realistic meter reference data with specifications and network topology
# - Simulated telemetry including power consumption, voltage, current, and power quality metrics  
# - Failure scenarios including individual meter failures and neighborhood-wide outages
# - Last gasp messages for battery failures and communication issues
# - Azure Event Hub integration for real-time data streaming
# 
# ## Requirements:
# - PySpark
# - Faker library for realistic data generation
# - Azure Event Hub SDK
# - Python datetime and random libraries

# MARKDOWN ********************

# ## 1. Setup PySpark and Import Libraries
# 
# Initialize PySpark session and import all required libraries for AMI telemetry simulation.

# CELL ********************

# Install required packages if not already installed
!pip install pyspark faker azure-eventhub pandas numpy

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

# MARKDOWN ********************

# # 2. Initialize Spark Session

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

# MARKDOWN ********************

# # 3. Retrieve  reference data from Lakehouse tables
# Retrieve previously-generated reference data from Lakehosue tables

# CELL ********************

#Retrieve reference data from Lakehouse tables
feeders_df = spark.sql("SELECT * FROM ReferenceDataLH.feeders")
meters_df = spark.sql("SELECT * FROM ReferenceDataLH.meters")
substations_df = spark.sql("SELECT * FROM ReferenceDataLH.substations")
transformers_df = spark.sql("SELECT * FROM ReferenceDataLH.transformers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Generate Realistic Telemetry Payloads
# 
# Create realistic AMI telemetry data including voltage, current, power consumption, power quality metrics, and timestamp information with appropriate data patterns.

# CELL ********************

import math
import builtins  # Import to access Python's built-in functions
from datetime import datetime, timedelta
from typing import Dict  # Add missing type hint import

def generate_realistic_load_profile(meter_type: str, hour: int, day_of_week: int, season: str) -> float:
    """Generate realistic power consumption based on meter type, time, and season"""
    
    base_loads = {
        "residential": {"summer": 2.5, "winter": 3.2, "spring": 1.8, "fall": 2.0},
        "commercial": {"summer": 15.0, "winter": 12.0, "spring": 10.0, "fall": 11.0},
        "industrial": {"summer": 45.0, "winter": 50.0, "spring": 42.0, "fall": 44.0}
    }
    
    base_load = base_loads[meter_type][season]
    
    if meter_type == "residential":
        # Residential load patterns
        if day_of_week < 5:  # Weekday
            if 6 <= hour <= 8 or 17 <= hour <= 22:  # Morning and evening peaks
                multiplier = 1.4 + random.uniform(-0.2, 0.3)
            elif 9 <= hour <= 16:  # Daytime low
                multiplier = 0.6 + random.uniform(-0.1, 0.2)
            elif 23 <= hour or hour <= 5:  # Night low
                multiplier = 0.3 + random.uniform(-0.1, 0.1)
            else:
                multiplier = 1.0 + random.uniform(-0.2, 0.2)
        else:  # Weekend
            if 8 <= hour <= 11 or 18 <= hour <= 21:  # Weekend peaks
                multiplier = 1.2 + random.uniform(-0.1, 0.2)
            elif 12 <= hour <= 17:  # Afternoon
                multiplier = 1.0 + random.uniform(-0.15, 0.15)
            else:
                multiplier = 0.7 + random.uniform(-0.1, 0.1)
    
    elif meter_type == "commercial":
        # Commercial load patterns
        if day_of_week < 5:  # Weekday
            if 7 <= hour <= 18:  # Business hours
                multiplier = 1.0 + random.uniform(-0.1, 0.2)
            else:  # After hours
                multiplier = 0.3 + random.uniform(-0.05, 0.1)
        else:  # Weekend
            multiplier = 0.2 + random.uniform(-0.05, 0.1)
    
    else:  # Industrial
        # Industrial - more consistent but with shifts
        if day_of_week < 5:  # Weekday
            if 6 <= hour <= 22:  # Operating hours
                multiplier = 0.9 + random.uniform(-0.1, 0.2)
            else:  # Night shift reduced
                multiplier = 0.7 + random.uniform(-0.1, 0.1)
        else:  # Weekend - reduced operations
            multiplier = 0.6 + random.uniform(-0.1, 0.1)
    
    # Add seasonal variations
    if season == "summer" and 12 <= hour <= 18:  # AC load
        multiplier *= 1.3
    elif season == "winter" and (6 <= hour <= 9 or 17 <= hour <= 21):  # Heating
        multiplier *= 1.2
    
    return base_load * multiplier

def calculate_electrical_parameters(power_kw: float, voltage_nominal: int, phases: int, power_factor: float = None) -> Dict:
    """Calculate realistic electrical parameters based on power consumption"""
    
    if power_factor is None or power_factor == 0:
        # Realistic power factors by load type
        if power_kw < 5:  # Residential
            power_factor = 0.95 + random.uniform(-0.05, 0.03)
        elif power_kw < 25:  # Small commercial
            power_factor = 0.92 + random.uniform(-0.08, 0.05)
        else:  # Large commercial/industrial
            power_factor = 0.88 + random.uniform(-0.10, 0.08)
        
        # Use Python's built-in min and max functions
        power_factor = builtins.max(0.75, builtins.min(0.99, power_factor))  # Realistic bounds
    
    # Calculate apparent power
    apparent_power_kva = power_kw / power_factor
    
    # Calculate reactive power
    reactive_power_kvar = math.sqrt(apparent_power_kva**2 - power_kw**2)
    
    # Calculate current (simplified for demonstration)
    if phases == 1:
        current_amps = (apparent_power_kva * 1000) / voltage_nominal
    else:  # 3-phase
        current_amps = (apparent_power_kva * 1000) / (voltage_nominal * math.sqrt(3))
    
    # Add realistic voltage variations (±5% of nominal)
    voltage_actual = voltage_nominal * (1 + random.uniform(-0.05, 0.05))
    
    # Power quality metrics
    thd_voltage = random.uniform(0.5, 3.0)  # Total Harmonic Distortion
    thd_current = random.uniform(1.0, 8.0)
    frequency = 60.0 + random.uniform(-0.1, 0.1)  # Grid frequency variation
    
    return {
        "active_power_kw": builtins.round(power_kw, 3),
        "reactive_power_kvar": builtins.round(reactive_power_kvar, 3),
        "apparent_power_kva": builtins.round(apparent_power_kva, 3),
        "current_amps": builtins.round(current_amps, 2),
        "voltage_volts": builtins.round(voltage_actual, 1),
        "power_factor": builtins.round(power_factor, 3),
        "frequency_hz": builtins.round(frequency, 2),
        "thd_voltage_percent": builtins.round(thd_voltage, 2),
        "thd_current_percent": builtins.round(thd_current, 2)
    }

def generate_telemetry_reading(meter: Dict, timestamp: datetime, season: str) -> Dict:
    """Generate a single telemetry reading for a meter"""
    
    hour = timestamp.hour
    day_of_week = timestamp.weekday()
    
    # Generate realistic power consumption
    power_kw = generate_realistic_load_profile(meter["meter_type"], hour, day_of_week, season)
    
    # Calculate electrical parameters
    electrical_params = calculate_electrical_parameters(
        power_kw, meter["voltage"], meter["phases"], 0
    )
    
    # Energy accumulation (simplified)
    energy_kwh_delivered = power_kw * (15/60)  # 15-minute interval
    energy_kwh_received = 0  # Assume no generation for most meters
    
    # Occasionally simulate solar generation for residential meters
    if meter["meter_type"] == "residential" and random.random() < 0.15:  # 15% have solar
        if 8 <= hour <= 17 and season in ["spring", "summer"]:  # Daylight hours
            solar_generation = random.uniform(0.5, 4.0)  # kW
            energy_kwh_received = solar_generation * (15/60)
            if solar_generation > power_kw:
                electrical_params["active_power_kw"] = -(solar_generation - power_kw)  # Net export
    
    # Device status information
    signal_strength = random.randint(-85, -45)  # dBm
    battery_voltage = 3.6 + random.uniform(-0.2, 0.1) if meter["communication_type"] in ["RF Mesh", "Cellular"] else None
    temperature_c = random.uniform(15, 45)  # Internal temperature
    
    # Tamper and alarm status
    tamper_detected = random.random() < 0.001  # 0.1% chance
    power_outage = random.random() < 0.002  # 0.2% chance
    
    telemetry = {
        "meter_id": meter["meter_id"],
        "timestamp": timestamp.isoformat(),
        "message_type": "interval_reading",
        "interval_minutes": 1,
        
        # Power measurements
        **electrical_params,
        
        # Energy measurements
        "energy_delivered_kwh": builtins.round(energy_kwh_delivered, 4),
        "energy_received_kwh": builtins.round(energy_kwh_received, 4),
        "energy_net_kwh": builtins.round(energy_kwh_delivered - energy_kwh_received, 4),
        
        # Cumulative energy (simplified simulation)
        "cumulative_energy_kwh": builtins.round(random.uniform(1000, 50000), 2),
        
        # Device status
        "signal_strength_dbm": signal_strength,
        "battery_voltage": battery_voltage,
        "internal_temperature_c": builtins.round(temperature_c, 1),
        
        # Status flags
        "tamper_detected": tamper_detected,
        "power_outage": power_outage,
        "communication_error": False,
        "meter_error": False,
               
        # Message metadata
        "sequence_number": random.randint(1000, 9999),
        "data_quality": "good",
        "firmware_version": meter["firmware_version"]
    }
    
    return telemetry

# Test telemetry generation
print("Testing telemetry generation...")
sample_meter = meters_df.first().asDict()
sample_timestamp = datetime.now()
sample_telemetry = generate_telemetry_reading(sample_meter, sample_timestamp, "summer")

print("Sample telemetry reading:")
for key, value in sample_telemetry.items():
    print(f"  {key}: {value}")

print(f"\nSample meter: {sample_meter['meter_id']} ({sample_meter['meter_type']})")
print(f"Timestamp: {sample_timestamp}")
print(f"Power consumption: {sample_telemetry['active_power_kw']} kW")
print(f"Voltage: {sample_telemetry['voltage_volts']} V")
print(f"Current: {sample_telemetry['current_amps']} A")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Implement Failure Scenarios and Last Gasp Messages
# 
# Simulate individual meter failures with last gasp messages, including battery low conditions, communication failures, and tamper alerts.

# CELL ********************

import builtins  # Import to access Python's built-in functions

class MeterFailureSimulator:
    """Simulate various meter failure scenarios"""
    
    def __init__(self):
        self.failed_meters = set()
        self.battery_low_meters = set()
        self.communication_error_meters = set()
        self.tamper_detected_meters = set()
        
        # Failure probabilities per hour
        self.failure_rates = {
            "battery_failure": 0.00001,      # Very rare
            "communication_failure": 0.0001,  # Uncommon
            "tamper_detection": 0.000005,     # Very rare
            "meter_malfunction": 0.000002,    # Extremely rare
            "power_supply_failure": 0.00002   # Rare
        }
    
    def generate_last_gasp_message(self, meter: Dict, failure_type: str, timestamp: datetime) -> Dict:
        """Generate a last gasp message for a failing meter"""
        
        # Battery information for last gasp
        if failure_type == "battery_failure":
            battery_voltage = random.uniform(2.8, 3.1)  # Critical battery level
            alert_code = "BATT_LOW"
            description = "Battery voltage critically low"
        elif failure_type == "power_supply_failure":
            battery_voltage = random.uniform(3.0, 3.4)  # Using backup battery
            alert_code = "PWR_FAIL"
            description = "Primary power supply failure, running on backup battery"
        elif failure_type == "communication_failure":
            battery_voltage = random.uniform(3.2, 3.6) if meter["communication_type"] in ["RF Mesh", "Cellular"] else None
            alert_code = "COMM_FAIL"
            description = "Communication module failure"
        elif failure_type == "tamper_detection":
            battery_voltage = random.uniform(3.3, 3.6) if meter["communication_type"] in ["RF Mesh", "Cellular"] else None
            alert_code = "TAMPER"
            description = "Meter tamper detected"
        else:  # meter_malfunction
            battery_voltage = random.uniform(3.0, 3.6) if meter["communication_type"] in ["RF Mesh", "Cellular"] else None
            alert_code = "METER_ERR"
            description = "Meter malfunction detected"
        
        last_gasp = {
            "meter_id": meter["meter_id"],
            "timestamp": timestamp.isoformat(),
            "message_type": "last_gasp",
            "alert_code": alert_code,
            "alert_description": description,
            "failure_type": failure_type,
            
            # Final readings before failure
            "final_energy_reading_kwh": builtins.round(random.uniform(1000, 50000), 2),
            "signal_strength_dbm": random.randint(-95, -85),  # Weak signal
            "battery_voltage": battery_voltage,
            "internal_temperature_c": builtins.round(random.uniform(20, 60), 1),
            
            # Diagnostic information
            "error_codes": [random.randint(1000, 9999) for _ in range(random.randint(1, 3))],
            "retry_attempts": random.randint(3, 10),
            "last_successful_transmission": (timestamp - timedelta(hours=random.randint(1, 48))).isoformat(),
            
            # Metadata
            "sequence_number": random.randint(1000, 9999),
            "firmware_version": meter["firmware_version"],
            "priority": "high",
            "requires_investigation": True
        }
        
        return last_gasp
    
    def simulate_individual_failures(self, meters_list: List[Dict], timestamp: datetime) -> List[Dict]:
        """Simulate random individual meter failures"""
        
        failure_messages = []
        
        for meter in meters_list:
            meter_id = meter["meter_id"]
            
            # Skip if meter already failed
            if meter_id in self.failed_meters:
                continue
            
            # Check for various failure types
            for failure_type, rate in self.failure_rates.items():
                if random.random() < rate:
                    # Generate last gasp message
                    last_gasp = self.generate_last_gasp_message(meter, failure_type, timestamp)
                    failure_messages.append(last_gasp)
                    
                    # Mark meter as failed (except for tamper which might be temporary)
                    if failure_type != "tamper_detection":
                        self.failed_meters.add(meter_id)
                    else:
                        self.tamper_detected_meters.add(meter_id)
                    
                    print(f"FAILURE: {meter_id} - {failure_type} at {timestamp}")
                    break  # Only one failure type per meter at a time
        
        return failure_messages
    
    def generate_battery_low_warning(self, meter: Dict, timestamp: datetime) -> Dict:
        """Generate battery low warning (before complete failure)"""
        
        warning = {
            "meter_id": meter["meter_id"],
            "timestamp": timestamp.isoformat(),
            "message_type": "battery_warning",
            "alert_code": "BATT_WARN",
            "alert_description": "Battery voltage low - maintenance required",
            
            "battery_voltage": builtins.round(random.uniform(3.1, 3.3), 2),
            "estimated_remaining_hours": random.randint(24, 168),  # 1-7 days
            "signal_strength_dbm": random.randint(-80, -60),
            "internal_temperature_c": builtins.round(random.uniform(20, 45), 1),
           
            "sequence_number": random.randint(1000, 9999),
            "firmware_version": meter["firmware_version"],
            "priority": "medium",
            "requires_maintenance": True
        }
        
        return warning
    
    def simulate_battery_warnings(self, meters_list: List[Dict], timestamp: datetime) -> List[Dict]:
        """Simulate battery low warnings"""
        
        warning_messages = []
        battery_warning_rate = 0.0002  # Higher rate for warnings than failures
        
        for meter in meters_list:
            meter_id = meter["meter_id"]
            
            # Only for wireless meters and those not already failed
            if (meter["communication_type"] in ["RF Mesh", "Cellular"] and 
                meter_id not in self.failed_meters and 
                meter_id not in self.battery_low_meters):
                
                if random.random() < battery_warning_rate:
                    warning = self.generate_battery_low_warning(meter, timestamp)
                    warning_messages.append(warning)
                    self.battery_low_meters.add(meter_id)
                    print(f"WARNING: {meter_id} - battery low at {timestamp}")
        
        return warning_messages

# Initialize failure simulator
failure_simulator = MeterFailureSimulator()

# Test individual failure simulation
print("Testing individual failure simulation...")
meters_list = meters_df.collect()
test_timestamp = datetime.now()

# Simulate failures for a small subset to test
test_failures = failure_simulator.simulate_individual_failures(meters_list[:100], test_timestamp)
test_warnings = failure_simulator.simulate_battery_warnings(meters_list[:100], test_timestamp)

print(f"Generated {len(test_failures)} failure messages")
print(f"Generated {len(test_warnings)} warning messages")

if test_failures:
    print("\nSample failure message:")
    for key, value in test_failures[0].items():
        print(f"  {key}: {value}")

if test_warnings:
    print("\nSample warning message:")
    for key, value in test_warnings[0].items():
        print(f"  {key}: {value}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Simulate Neighborhood-wide Outages
# 
# Implement correlated failures affecting multiple meters on the same feeder line or geographic area to simulate realistic grid disturbances.

# CELL ********************

class NeighborhoodOutageSimulator:
    """Simulate correlated outages affecting multiple meters"""
    
    def __init__(self):
        self.active_outages = {}  # feeder_id -> outage_info
        self.outage_history = []
    
    def generate_outage_event(self, outage_type: str, affected_component: str, 
                            affected_meters: List[Dict], timestamp: datetime) -> Dict:
        """Generate an outage event affecting multiple meters"""

        outage_event = {
            "outage_id": str(uuid.uuid4()),
            "outage_type": outage_type,
            "start_time": timestamp.isoformat(),
            "priority": "high"
        }
        
        return outage_event
    
    def generate_outage_last_gasp_messages(self, affected_meters: List[Dict], 
                                         outage_event: Dict, timestamp: datetime) -> List[Dict]:
        """Generate last gasp messages for all meters affected by outage"""
        
        messages = []
        
        for meter in affected_meters:
            # Stagger the last gasp messages slightly to simulate realistic timing
            gasp_timestamp = timestamp + timedelta(seconds=random.randint(0, 30))
            
            last_gasp = {
                "meter_id": meter["meter_id"],
                "timestamp": gasp_timestamp.isoformat(),
                "message_type": "outage_last_gasp",
                "outage_id": outage_event["outage_id"],
                "alert_code": "POWER_OUT",
                "alert_description": "Power outage detected",
                
                # Power information at time of outage
                "voltage_at_outage": 0.0,  # No voltage during outage
                "last_voltage_reading": builtins.round(meter["voltage"] * (1 + random.uniform(-0.02, 0.02)), 1),
                "power_at_outage": 0.0,
                
                # Battery backup information (for wireless meters)
                "battery_voltage": (builtins.round(random.uniform(3.2, 3.6), 2) 
                                  if meter["communication_type"] in ["RF Mesh", "Cellular"] else None),
                "backup_power_available": meter["communication_type"] in ["RF Mesh", "Cellular"],
                "estimated_backup_hours": (random.randint(12, 72) 
                                         if meter["communication_type"] in ["RF Mesh", "Cellular"] else 0),
                
                # Outage correlation information
                "outage_type": outage_event["outage_type"],
                "correlated_outage": True,
                
                # Metadata
                "sequence_number": random.randint(1000, 9999),
                "firmware_version": meter["firmware_version"],
                "priority": "high",
                "automatic_restoration_expected": True
            }
            
            messages.append(last_gasp)
        
        return messages
    
    def simulate_feeder_outage(self, meters_df: DataFrame, affected_feeders_df: DataFrame,
                             timestamp: datetime) -> Tuple[Dict, List[Dict]]:
        """Simulate an outage affecting an entire feeder"""
        affected_feeders = [row.feeder_id for row in affected_feeders_df.collect()]

        # Select a random feeder
        for affected_feeder in affected_feeders:  
            #Get all transformers on this feeder
            affected_transformers = transformers_df.filter(col("feeder_id") == affected_feeder)
            affected_transformers = [row.transformer_id for row in affected_transformers.collect()]

            # Get all meters on this feeder
            affected_meters = meters_df.filter(col("transformer_id").isin(affected_transformers)).collect()
            affected_meters = [meter.asDict() for meter in affected_meters]
            
            # Generate outage event
            outage_event = self.generate_outage_event("feeder_outage", affected_feeder, 
                                                    affected_meters, timestamp)
        
            # Generate last gasp messages
            last_gasp_messages = self.generate_outage_last_gasp_messages(
                affected_meters, outage_event, timestamp)
            
            # Track active outage
            self.active_outages[affected_feeder] = outage_event
            self.outage_history.append(outage_event)
            
            print(f"Generated {len(last_gasp_messages)} last gasp messages")

            return outage_event, last_gasp_messages
    
    def simulate_transformer_outage(self, meters_df: DataFrame, affected_transformers_df: DataFrame, 
                                  timestamp: datetime) -> Tuple[Dict, List[Dict]]:
        """Simulate an outage affecting meters on one transformer"""
        affected_transformers = [row.transformer_id for row in affected_transformers_df.collect()]

        for affected_transformer in affected_transformers:           
            # Get all meters on this transformer
            affected_meters = meters_df.filter(col("transformer_id") == affected_transformer).collect()
            affected_meters = [meter.asDict() for meter in affected_meters]
            
            # Generate outage event
            outage_event = self.generate_outage_event("transformer_outage", affected_transformer, 
                                                    affected_meters, timestamp)
            
            # Generate last gasp messages
            last_gasp_messages = self.generate_outage_last_gasp_messages(
                affected_meters, outage_event, timestamp)
            
            # Track active outage
            self.active_outages[affected_transformer] = outage_event
            self.outage_history.append(outage_event)
            
            print(f"Generated {len(last_gasp_messages)} last gasp messages")

            return outage_event, last_gasp_messages
    
    def simulate_restoration_messages(self, outage_event: Dict, meters_df: DataFrame, 
                                    timestamp: datetime) -> List[Dict]:
        """Generate restoration messages when power is restored"""
        
        # Get affected meters
        affected_meter_ids = outage_event["affected_meter_ids"]
        affected_meters = meters_df.filter(col("meter_id").isin(affected_meter_ids)).collect()
        
        restoration_messages = []
        
        for meter in affected_meters:
            meter_dict = meter.asDict()
            
            # Stagger restoration messages
            restore_timestamp = timestamp + timedelta(seconds=random.randint(0, 120))
            
            restoration = {
                "meter_id": meter_dict["meter_id"],
                "timestamp": restore_timestamp.isoformat(),
                "message_type": "power_restoration",
                "outage_id": outage_event["outage_id"],
                "alert_code": "POWER_RESTORED",
                "alert_description": "Power restored after outage",
                
                # Power restoration information
                "voltage_restored": builtins.round(meter_dict["voltage"] * (1 + random.uniform(-0.03, 0.03)), 1),
                "power_restored": True,
                "outage_duration_minutes": int((timestamp - datetime.fromisoformat(outage_event["start_time"])).total_seconds() / 60),
                
                # System status after restoration
                "meter_self_test_passed": random.choice([True, True, True, True, True, False]),  # Usually passes
                "clock_sync_required": random.choice([True, False]),
                "data_recovery_successful": random.choice([True, True, False]),  # Usually successful
                
                # Metadata
                "sequence_number": random.randint(1000, 9999),
                "firmware_version": meter_dict["firmware_version"],
                "priority": "medium",
                "automatic_readings_resumed": True
            }
            
            restoration_messages.append(restoration)
        
        return restoration_messages

# Initialize neighborhood outage simulator
outage_simulator = NeighborhoodOutageSimulator()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test neighborhood outage simulation
print("Testing neighborhood outage simulation...")
affected_feeders_df = feeders_df.limit(1)
affected_feeders_df.show()

test_timestamp = datetime.now()
outage_event, last_gasp_msgs = outage_simulator.simulate_feeder_outage(
    meters_df, affected_feeders_df, test_timestamp)

print(f"Outage type: {outage_event['outage_type']}")
print(f"Generated {len(last_gasp_msgs)} last gasp messages")

#Sample last gasp message
if last_gasp_msgs:
    print("\nSample outage last gasp message:")
    sample_msg = last_gasp_msgs[1]
    for key, value in sample_msg.items():
        print(f"  {key}: {value}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Configure Azure Event Hub Connection
# 
# Set up Azure Event Hub connection parameters, authentication, and message formatting for AMI telemetry transmission.

# CELL ********************

# Configure Azure Event Hub connection parameters
AZURE_EVENTHUB_CONNECTION_STRING = spark.sql("SELECT value FROM ReferenceDataLH.secrets WHERE name = 'AMI-Telemetry-EH-ConnectionString'").first()[0]
AZURE_EVENTHUB_NAME = "es_ccfc7b94-77c3-4083-a51c-9fd88f7a0394"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class AMIEventHubClient:
    """Azure Event Hub client for AMI telemetry data"""
    
    def __init__(self, connection_string: str = None, event_hub_name: str = None):
        """Initialize Event Hub client
        
        Args:
            connection_string: Azure Event Hub connection string
            event_hub_name: Name of the Event Hub
        """
        self.connection_string = connection_string or self._get_connection_string()
        self.event_hub_name = event_hub_name or "ami-telemetry"
        self.producer_client = None
        self.message_count = 0
        self.error_count = 0
        
        if AZURE_AVAILABLE and self.connection_string:
            try:
                self.producer_client = EventHubProducerClient.from_connection_string(
                    conn_str=self.connection_string,
                    eventhub_name=self.event_hub_name
                )
                print("Azure Event Hub client initialized successfully")
            except Exception as e:
                print(f"Failed to initialize Event Hub client: {e}")
                self.producer_client = None
        else:
            print("Azure Event Hub not available or not configured - using simulation mode")
    
    def _get_connection_string(self) -> str:
        """Get connection string from environment or return None for demo mode"""
        # In production, get from environment variable or Azure Key Vault
        connection_string = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
        
        if not connection_string:
            # Demo/simulation mode - return None
            print("No Azure Event Hub connection string found.")
            print("Set AZURE_EVENTHUB_CONNECTION_STRING environment variable for real Event Hub.")
            print("Running in simulation mode...")
            return None
        
        return connection_string
    
    def format_message_for_eventhub(self, telemetry_data: Dict) -> Dict:
        """Format telemetry data for Event Hub transmission"""
        
        # Add Event Hub specific metadata
        formatted_message = {
            "messageId": str(uuid.uuid4()),
            "messageType": telemetry_data.get("message_type", "unknown"),
            "deviceId": telemetry_data["meter_id"],
            "timestamp": telemetry_data["timestamp"],
            "version": "1.1",
            "source": "AMI_Capture",
            
            # Core telemetry payload
            "telemetry": telemetry_data,
            
            # Message routing information
            "partitionKey": telemetry_data["meter_id"],  # Route by meter ID
            "priority": telemetry_data.get("priority", "normal"),
            
            # Correlation information for outages
            "correlationId": telemetry_data.get("outage_id", None)
        }
        
        return formatted_message
    
    def send_message(self, telemetry_data: Dict) -> bool:
        """Send a single telemetry message to Event Hub"""
        
        try:
            formatted_message = self.format_message_for_eventhub(telemetry_data)
            message_json = json.dumps(formatted_message)
            
            if self.producer_client:
                # Send to actual Azure Event Hub
                event_data = EventData(message_json)
                
                # Set partition key for load balancing
                event_data.properties = {
                    "meter_id": telemetry_data["meter_id"],
                    "message_type": telemetry_data.get("message_type", "unknown"),
                    "priority": telemetry_data.get("priority", "normal")
                }
                
                with self.producer_client:
                    event_data_batch = self.producer_client.create_batch()
                    event_data_batch.add(event_data)
                    self.producer_client.send_batch(event_data_batch)
                
                self.message_count += 1
                return True
            else:
                # Simulation mode - just log the message
                print(f"SIMULATION: Would send message from {telemetry_data['meter_id']} " +
                      f"({telemetry_data.get('message_type', 'unknown')}) to Event Hub")
                self.message_count += 1
                return True
                
        except Exception as e:
            print(f"Error sending message: {e}")
            self.error_count += 1
            return False
    
    def send_batch(self, telemetry_batch: List[Dict]) -> Dict:
        """Send a batch of telemetry messages to Event Hub"""
        
        success_count = 0
        failure_count = 0
        
        try:
            if self.producer_client:
                # Send to actual Azure Event Hub in batches
                formatted_messages = [self.format_message_for_eventhub(data) for data in telemetry_batch]
                
                with self.producer_client:
                    event_data_batch = self.producer_client.create_batch()
                    
                    for telemetry_data, formatted_msg in zip(telemetry_batch, formatted_messages):
                        try:
                            message_json = json.dumps(formatted_msg)
                            event_data = EventData(message_json)
                            
                            # Set partition key
                            event_data.properties = {
                                "meter_id": telemetry_data["meter_id"],
                                "message_type": telemetry_data.get("message_type", "unknown")
                            }
                            
                            event_data_batch.add(event_data)
                            success_count += 1
                            
                        except Exception as e:
                            print(f"Error adding message to batch: {e}")
                            failure_count += 1
                    
                    if event_data_batch:
                        self.producer_client.send_batch(event_data_batch)
                        self.message_count += success_count
            else:
                # Simulation mode
                for telemetry_data in telemetry_batch:
                    if self.send_message(telemetry_data):
                        success_count += 1
                    else:
                        failure_count += 1
                        
        except Exception as e:
            print(f"Error sending batch: {e}")
            failure_count = len(telemetry_batch) - success_count
            self.error_count += failure_count
        
        return {
            "total_messages": len(telemetry_batch),
            "successful": success_count,
            "failed": failure_count,
            "success_rate": success_count / len(telemetry_batch) if telemetry_batch else 0
        }
    
    def get_stats(self) -> Dict:
        """Get transmission statistics"""
        return {
            "total_messages_sent": self.message_count,
            "total_errors": self.error_count,
            "success_rate": (self.message_count / (self.message_count + self.error_count) 
                           if (self.message_count + self.error_count) > 0 else 0),
            "event_hub_connected": self.producer_client is not None
        }
    
    def close(self):
        """Close the Event Hub client"""
        if self.producer_client:
            self.producer_client.close()

# Configuration for Azure Event Hub
EVENT_HUB_CONFIG = {
    "connection_string": AZURE_EVENTHUB_CONNECTION_STRING,
    "event_hub_name": AZURE_EVENTHUB_NAME,
    "batch_size": 500,  # Messages per batch
    "send_interval_seconds": 1,  # Send every 5 seconds
    "retry_attempts": 3,
    "timeout_seconds": 1
}

print("Azure Event Hub Configuration:")
for key, value in EVENT_HUB_CONFIG.items():
    if key == "connection_string" and value:
        print(f"  {key}: [CONFIGURED]")
    else:
        print(f"  {key}: {value}")

# Initialize Event Hub client
event_hub_client = AMIEventHubClient(
    connection_string=EVENT_HUB_CONFIG["connection_string"],
    event_hub_name=EVENT_HUB_CONFIG["event_hub_name"]
)

# Test message formatting
test_telemetry = {
    "meter_id": "MTR000001",
    "timestamp": datetime.now().isoformat(),
    "message_type": "interval_reading",
    "active_power_kw": 3.5,
    "voltage_volts": 240.0,
    "current_amps": 14.6
}

formatted_test = event_hub_client.format_message_for_eventhub(test_telemetry)
print("\\nSample formatted Event Hub message:")
print(json.dumps(formatted_test, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Send Telemetry Data to Event Hub
# 
# Implement the data transmission logic to send simulated telemetry and failure messages to Azure Event Hub in real-time or batch mode.

# CELL ********************

import time
from concurrent.futures import ThreadPoolExecutor
import threading

class AMITelemetryOrchestrator:
    """Orchestrates the complete AMI telemetry simulation and transmission"""
    
    def __init__(self, meters_df: DataFrame, event_hub_client: AMIEventHubClient,
                 failure_simulator: MeterFailureSimulator, outage_simulator: NeighborhoodOutageSimulator):
        self.meters_df = meters_df
        self.event_hub_client = event_hub_client
        self.failure_simulator = failure_simulator
        self.outage_simulator = outage_simulator
        
        self.simulation_running = False
        self.stats = {
            "total_readings_generated": 0,
            "total_failures": 0,
            "total_outages": 0,
            "total_messages_sent": 0,
            "start_time": None,
            "last_batch_time": None
        }
    
    def determine_season(self, timestamp: datetime) -> str:
        """Determine season based on timestamp"""
        month = timestamp.month
        if month in [12, 1, 2]:
            return "winter"
        elif month in [3, 4, 5]:
            return "spring"
        elif month in [6, 7, 8]:
            return "summer"
        else:
            return "fall"
    
    def generate_telemetry_batch(self, timestamp: datetime, meters_sample: List[Dict] = None) -> List[Dict]:
        """Generate a batch of telemetry readings for all active meters"""
        
        if meters_sample is None:
            # Get all active meters (not failed)
            all_meters = self.meters_df.collect()
            active_meters = [m.asDict() for m in all_meters 
                           if m.meter_id not in self.failure_simulator.failed_meters]
        else:
            active_meters = meters_sample
        
        season = self.determine_season(timestamp)
        telemetry_batch = []
        
        for meter in active_meters:
            try:
                reading = generate_telemetry_reading(meter, timestamp, season)
                telemetry_batch.append(reading)
                self.stats["total_readings_generated"] += 1
            except Exception as e:
                print(f"Error generating reading for meter {meter['meter_id']}: {e}")
        
        return telemetry_batch
    
    def simulate_failures_and_outages(self, timestamp: datetime) -> List[Dict]:
        """Simulate various failure scenarios and return failure messages"""
        
        failure_messages = []
        
        # Get active meters for failure simulation
        all_meters = self.meters_df.collect()
        active_meters = [m.asDict() for m in all_meters 
                        if m.meter_id not in self.failure_simulator.failed_meters]
        
        # Simulate individual meter failures
        individual_failures = self.failure_simulator.simulate_individual_failures(active_meters, timestamp)
        failure_messages.extend(individual_failures)
        self.stats["total_failures"] += len(individual_failures)
        
        # Simulate battery warnings
        battery_warnings = self.failure_simulator.simulate_battery_warnings(active_meters, timestamp)
        failure_messages.extend(battery_warnings)
        
        # Simulate neighborhood outages (less frequent)

        # Read data from KQL to get transformers and feeders affected by storms
        # The query URI for reading the data e.g. https://<>.kusto.data.microsoft.com.
        kusto_uri = "https://trd-wd4wk1apq1ku3uh8p5.z1.kusto.fabric.microsoft.com"
        # The database with data to be read.
        kusto_database = "PowerUtilitiesEH"
        # The access credentials.
        kusto_access_token = mssparkutils.credentials.getToken('kusto')

        transformers_query = '''
        GetTransformersAffectedByLatestStorm(10)
        '''
        affected_transformers_df = spark.read\
            .format("com.microsoft.kusto.spark.synapse.datasource")\
            .option("accessToken", kusto_access_token)\
            .option("kustoCluster", kusto_uri)\
            .option("kustoDatabase", kusto_database)\
            .option("kustoQuery", transformers_query).load()

        feeders_query = '''
        GetFeedersAffectedByLatestStorm(5)
        '''
        affected_feeders_df = spark.read\
            .format("com.microsoft.kusto.spark.synapse.datasource")\
            .option("accessToken", kusto_access_token)\
            .option("kustoCluster", kusto_uri)\
            .option("kustoDatabase", kusto_database)\
            .option("kustoQuery", feeders_query).load()


        if affected_feeders_df.count() > 0:
            outage_type = "feeder_outage"
            outage_event, outage_messages = self.outage_simulator.simulate_feeder_outage(
                    self.meters_df, affected_feeders_df, timestamp)
            
            failure_messages.extend(outage_messages)
            self.stats["total_outages"] += 1
            
            print(f"OUTAGE SIMULATED: {outage_event['outage_type']}")        
        
        if affected_transformers_df.count() > 0:
            outage_type = "transformer_outage"
            outage_event, outage_messages = self.outage_simulator.simulate_transformer_outage(
                    self.meters_df, affected_transformers_df, timestamp)
            
            failure_messages.extend(outage_messages)
            self.stats["total_outages"] += 1
            
            print(f"OUTAGE SIMULATED: {outage_event['outage_type']}")
        
        return failure_messages
    
    def send_telemetry_batch(self, telemetry_batch: List[Dict], failure_messages: List[Dict]) -> Dict:
        """Send telemetry and failure messages to Event Hub"""
        
        all_messages = telemetry_batch + failure_messages
        
        if not all_messages:
            return {"total_messages": 0, "successful": 0, "failed": 0}
        
        # Send in smaller batches to avoid Event Hub limits
        batch_size = EVENT_HUB_CONFIG["batch_size"]
        total_results = {"total_messages": 0, "successful": 0, "failed": 0}
        
        for i in range(0, len(all_messages), batch_size):
            batch = all_messages[i:i + batch_size]
            result = self.event_hub_client.send_batch(batch)
            
            total_results["total_messages"] += result["total_messages"]
            total_results["successful"] += result["successful"]
            total_results["failed"] += result["failed"]
            
            # Small delay between batches to avoid overwhelming Event Hub
            if len(all_messages) > batch_size:
                time.sleep(0.1)
        
        self.stats["total_messages_sent"] += total_results["successful"]
        return total_results
    
    def run_simulation_cycle(self, timestamp: datetime, sample_size: int = None) -> Dict:
        """Run one complete simulation cycle"""
        
        cycle_start = time.time()
        
        # Sample meters if requested (for performance testing)
        meters_sample = None
        if sample_size:
            all_meters = self.meters_df.collect()
            active_meters = [m.asDict() for m in all_meters 
                           if m.meter_id not in self.failure_simulator.failed_meters]
            meters_sample = random.sample(active_meters, builtins.min(sample_size, len(active_meters)))
        
        # Generate telemetry batch
        telemetry_batch = self.generate_telemetry_batch(timestamp, meters_sample)
        
        # Simulate failures and outages
        failure_messages = self.simulate_failures_and_outages(timestamp)
        
        # Send to Event Hub
        send_result = self.send_telemetry_batch(telemetry_batch, failure_messages)
        
        cycle_time = time.time() - cycle_start
        
        result = {
            "timestamp": timestamp.isoformat(),
            "cycle_duration_seconds": builtins.round(cycle_time, 2),
            "telemetry_readings": len(telemetry_batch),
            "failure_messages": len(failure_messages),
            "transmission_result": send_result,
            "active_meters": len(telemetry_batch) if telemetry_batch else 0,
            "failed_meters": len(self.failure_simulator.failed_meters)
        }
        
        self.stats["last_batch_time"] = timestamp
        return result
    
    def run_continuous_simulation(self, duration_minutes: int = 60, 
                                interval_minutes: int = 1, sample_size: int = None):
        """Run continuous telemetry simulation"""
        
        print(f"Starting continuous AMI telemetry simulation...")
        print(f"Duration: {duration_minutes} minutes")
        print(f"Interval: {interval_minutes} minutes")
        print(f"Sample size: {sample_size or 'All meters'}")

        
        self.simulation_running = True
        self.stats["start_time"] = datetime.now()
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        current_time = start_time
        
        cycle_count = 0
        
        try:
            while current_time < end_time and self.simulation_running:
                cycle_count += 1
                
                print(f"\\nCycle {cycle_count} - {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Run simulation cycle
                result = self.run_simulation_cycle(current_time, sample_size)
                
                # Print results
                print(f"  Generated {result['telemetry_readings']} telemetry readings")
                print(f"  Generated {result['failure_messages']} failure messages")
                #print(f"  Sent {result['transmission_result']['successful']} messages successfully")
                #if result['transmission_result']['failed'] > 0:
                #    print(f"  Failed to send {result['transmission_result']['failed']} messages")
                #print(f"  Active meters: {result['active_meters']}")
                #print(f"  Failed meters: {result['failed_meters']}")
                print(f"  Cycle duration: {result['cycle_duration_seconds']}s")
                
                # Move to next interval
                current_time += timedelta(minutes=interval_minutes)
                
                # Sleep until next interval (adjusted for processing time)
                if self.simulation_running and current_time < end_time:
                    sleep_seconds = builtins.max(0, interval_minutes * 60 - result['cycle_duration_seconds'])
                    if sleep_seconds > 0:
                        print(f"  Waiting {sleep_seconds:.1f}s until next cycle...")
                        time.sleep(sleep_seconds)
                
        except KeyboardInterrupt:
            print("\\nSimulation interrupted by user")
        finally:
            self.simulation_running = False
            
        print(f"\\nSimulation completed after {cycle_count} cycles")
        self.print_simulation_summary()
    
    def stop_simulation(self):
        """Stop the continuous simulation"""
        self.simulation_running = False
    
    def print_simulation_summary(self):
        """Print summary statistics"""
        print("\\n" + "="*50)
        print("AMI TELEMETRY SIMULATION SUMMARY")
        print("="*50)
        
        if self.stats["start_time"]:
            duration = datetime.now() - self.stats["start_time"]
            print(f"Simulation duration: {duration}")
        
        print(f"Total telemetry readings generated: {self.stats['total_readings_generated']:,}")
        print(f"Total failure events: {self.stats['total_failures']}")
        print(f"Total outage events: {self.stats['total_outages']}")
        print(f"Total messages sent to Event Hub: {self.stats['total_messages_sent']:,}")
        print(f"Currently failed meters: {len(self.failure_simulator.failed_meters)}")
        
        # Event Hub client stats
        eh_stats = self.event_hub_client.get_stats()
        print(f"Event Hub success rate: {eh_stats['success_rate']:.2%}")
        print(f"Event Hub connected: {eh_stats['event_hub_connected']}")

# Initialize the orchestrator
orchestrator = AMITelemetryOrchestrator(
    meters_df=meters_df,
    event_hub_client=event_hub_client,
    failure_simulator=failure_simulator,
    outage_simulator=outage_simulator
)

print("AMI Telemetry Orchestrator initialized successfully!")
print(f"Ready to simulate telemetry from {meters_df.count()} meters")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Monitor and Validate Data Transmission
# 
# Add monitoring and validation logic to track successful transmissions, handle failures, and verify data integrity in the Event Hub.

# CELL ********************

class AMIDataValidator:
    """Validate and monitor AMI telemetry data quality"""
    
    def __init__(self):
        self.validation_stats = {
            "total_messages_validated": 0,
            "validation_errors": 0,
            "data_quality_issues": 0,
            "schema_violations": 0,
            "value_range_errors": 0
        }
    
    def validate_telemetry_reading(self, reading: Dict) -> Dict:
        """Validate a single telemetry reading"""
        
        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "data_quality_score": 1.0
        }
        
        # Required fields validation
        required_fields = [
            "meter_id", "timestamp", "message_type", "active_power_kw",
            "voltage_volts", "current_amps"
        ]
        
        for field in required_fields:
            if field not in reading or reading[field] is None:
                validation_result["errors"].append(f"Missing required field: {field}")
                validation_result["is_valid"] = False
        
        if not validation_result["is_valid"]:
            return validation_result
        
        # Value range validations
        validations = [
            ("active_power_kw", 0, 1000, "Active power out of expected range"),
            ("voltage_volts", 100, 600, "Voltage out of expected range"),
            ("current_amps", 0, 1000, "Current out of expected range"),
            ("power_factor", 0.5, 1.0, "Power factor out of valid range"),
            ("frequency_hz", 59.5, 60.5, "Frequency out of grid standard range")
        ]
        
        for field, min_val, max_val, error_msg in validations:
            if field in reading:
                value = reading[field]
                if isinstance(value, (int, float)) and not (min_val <= value <= max_val):
                    validation_result["warnings"].append(f"{error_msg}: {value}")
                    validation_result["data_quality_score"] *= 0.9
        
        # Timestamp validation
        try:
            ts = datetime.fromisoformat(reading["timestamp"].replace("Z", "+00:00"))
            now = datetime.now()
            if builtins.abs((now - ts).total_seconds()) > 3600:  # More than 1 hour old
                validation_result["warnings"].append("Timestamp is more than 1 hour old")
                validation_result["data_quality_score"] *= 0.95
        except Exception as e:
            validation_result["errors"].append(f"Invalid timestamp format: {e}")
            validation_result["is_valid"] = False
        
        # Power calculation consistency check
        if all(field in reading for field in ["active_power_kw", "voltage_volts", "current_amps", "power_factor"]):
            calculated_power = (reading["voltage_volts"] * reading["current_amps"] * 
                              reading["power_factor"]) / 1000
            power_diff = builtins.abs(calculated_power - reading["active_power_kw"])
            
            if power_diff > reading["active_power_kw"] * 0.1:  # 10% tolerance
                validation_result["warnings"].append(
                    f"Power calculation inconsistency: reported={reading['active_power_kw']}, "
                    f"calculated={calculated_power:.2f}")
                validation_result["data_quality_score"] *= 0.85
        
        # Update statistics
        self.validation_stats["total_messages_validated"] += 1
        if not validation_result["is_valid"]:
            self.validation_stats["validation_errors"] += 1
        if validation_result["warnings"]:
            self.validation_stats["data_quality_issues"] += 1
        
        return validation_result
    
    def validate_batch(self, telemetry_batch: List[Dict]) -> Dict:
        """Validate a batch of telemetry readings"""
        
        batch_results = {
            "total_messages": len(telemetry_batch),
            "valid_messages": 0,
            "invalid_messages": 0,
            "warnings_count": 0,
            "average_quality_score": 0.0,
            "validation_details": []
        }
        
        total_quality_score = 0.0
        
        for reading in telemetry_batch:
            result = self.validate_telemetry_reading(reading)
            
            if result["is_valid"]:
                batch_results["valid_messages"] += 1
            else:
                batch_results["invalid_messages"] += 1
            
            if result["warnings"]:
                batch_results["warnings_count"] += 1
            
            total_quality_score += result["data_quality_score"]
            batch_results["validation_details"].append({
                "meter_id": reading.get("meter_id", "unknown"),
                "is_valid": result["is_valid"],
                "quality_score": result["data_quality_score"],
                "error_count": len(result["errors"]),
                "warning_count": len(result["warnings"])
            })
        
        if telemetry_batch:
            batch_results["average_quality_score"] = total_quality_score / len(telemetry_batch)
        
        return batch_results
    
    def get_validation_summary(self) -> Dict:
        """Get overall validation statistics"""
        return {
            **self.validation_stats,
            "validation_success_rate": (
                (self.validation_stats["total_messages_validated"] - 
                 self.validation_stats["validation_errors"]) /
                builtins.max(1, self.validation_stats["total_messages_validated"])
            ),
            "data_quality_rate": (
                (self.validation_stats["total_messages_validated"] - 
                 self.validation_stats["data_quality_issues"]) /
                builtins.max(1, self.validation_stats["total_messages_validated"])
            )
        }

class AMIMonitoringDashboard:
    """Real-time monitoring dashboard for AMI telemetry"""
    
    def __init__(self, validator: AMIDataValidator):
        self.validator = validator
        self.monitoring_stats = {
            "simulation_start_time": None,
            "last_update_time": None,
            "total_meters": 0,
            "active_meters": 0,
            "failed_meters": 0,
            "messages_per_minute": 0,
            "current_throughput": 0,
            "peak_throughput": 0,
            "outage_events": 0,
            "critical_alerts": 0
        }
    
    def update_metrics(self, orchestrator: AMITelemetryOrchestrator, 
                      cycle_result: Dict = None):
        """Update monitoring metrics"""
        
        current_time = datetime.now()
        
        if self.monitoring_stats["simulation_start_time"] is None:
            self.monitoring_stats["simulation_start_time"] = current_time
        
        self.monitoring_stats["last_update_time"] = current_time
        
        # Update from orchestrator stats
        self.monitoring_stats["total_meters"] = orchestrator.meters_df.count()
        self.monitoring_stats["failed_meters"] = len(orchestrator.failure_simulator.failed_meters)
        self.monitoring_stats["active_meters"] = (
            self.monitoring_stats["total_meters"] - self.monitoring_stats["failed_meters"]
        )
        self.monitoring_stats["outage_events"] = orchestrator.stats["total_outages"]
        
        # Calculate throughput if cycle result provided
        if cycle_result:
            cycle_duration = cycle_result["cycle_duration_seconds"]
            if cycle_duration > 0:
                messages_in_cycle = (cycle_result["telemetry_readings"] + 
                                   cycle_result["failure_messages"])
                current_throughput = messages_in_cycle / (cycle_duration / 60)  # per minute
                
                self.monitoring_stats["current_throughput"] = current_throughput
                if current_throughput > self.monitoring_stats["peak_throughput"]:
                    self.monitoring_stats["peak_throughput"] = current_throughput
        
        # Count critical alerts (failures + outages)
        self.monitoring_stats["critical_alerts"] = (
            orchestrator.stats["total_failures"] + orchestrator.stats["total_outages"]
        )
    
    def print_dashboard(self, show_details: bool = True):
        """Print monitoring dashboard"""
        
        print("\\n" + "="*60)
        print("AMI TELEMETRY MONITORING DASHBOARD")
        print("="*60)
        
        # System overview
        print("SYSTEM OVERVIEW:")
        print(f"  Total Meters: {self.monitoring_stats['total_meters']:,}")
        print(f"  Active Meters: {self.monitoring_stats['active_meters']:,}")
        print(f"  Failed Meters: {self.monitoring_stats['failed_meters']:,}")
        print(f"  System Availability: {(self.monitoring_stats['active_meters'] / builtins.max(1, self.monitoring_stats['total_meters'])):.2%}")
        
        # Performance metrics
        print(f"\\nPERFORMANCE METRICS:")
        print(f"  Current Throughput: {self.monitoring_stats['current_throughput']:.1f} messages/min")
        print(f"  Peak Throughput: {self.monitoring_stats['peak_throughput']:.1f} messages/min")
        
        # Alerts and events
        print(f"\\nALERTS & EVENTS:")
        print(f"  Active Outages: {self.monitoring_stats['outage_events']}")
        print(f"  Critical Alerts: {self.monitoring_stats['critical_alerts']}")
        
        # Data quality
        validation_summary = self.validator.get_validation_summary()
        print(f"\\nDATA QUALITY:")
        print(f"  Messages Validated: {validation_summary['total_messages_validated']:,}")
        print(f"  Validation Success Rate: {validation_summary['validation_success_rate']:.2%}")
        print(f"  Data Quality Rate: {validation_summary['data_quality_rate']:.2%}")
        
        # Timestamps
        if self.monitoring_stats["last_update_time"]:
            print(f"\\nTIMESTAMPS:")
            print(f"  Last Update: {self.monitoring_stats['last_update_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            if self.monitoring_stats["simulation_start_time"]:
                runtime = self.monitoring_stats["last_update_time"] - self.monitoring_stats["simulation_start_time"]
                print(f"  Total Runtime: {runtime}")
        
        print("="*60)

# Initialize validation and monitoring
validator = AMIDataValidator()
dashboard = AMIMonitoringDashboard(validator)

# Test validation with sample data
print("Testing data validation...")
sample_readings = []
for i in range(5):
    sample_meter = meters_df.collect()[i].asDict()
    reading = generate_telemetry_reading(sample_meter, datetime.now(), "summer")
    sample_readings.append(reading)

# Validate sample batch
validation_results = validator.validate_batch(sample_readings)
print(f"Validation Results:")
print(f"  Total messages: {validation_results['total_messages']}")
print(f"  Valid messages: {validation_results['valid_messages']}")
print(f"  Invalid messages: {validation_results['invalid_messages']}")
print(f"  Messages with warnings: {validation_results['warnings_count']}")
print(f"  Average quality score: {validation_results['average_quality_score']:.3f}")

# Update and display dashboard
#dashboard.update_metrics(orchestrator)
#dashboard.print_dashboard()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Running the Complete Simulation
# 
# ### Quick Start Examples
# 
# Below are examples of how to run simulations:

# CELL ********************

# Example: Extended simulation for production testing
print("\\n\\nExample 2: Extended Simulation Configuration")
print("-" * 50)

def run_extended_simulation(duration_hours: float = 24.0, sample_percentage: float = 1.0):
    """Run an extended simulation with monitoring"""
    
    total_meters = meters_df.count()
    sample_size = builtins.max(10, int(total_meters * sample_percentage))
    
    print(f"Starting extended simulation:")
    print(f"  Duration: {duration_hours} hours")
    print(f"  Sample size: {sample_size} meters ({sample_percentage:.1%} of total)")
    
    # Run the simulation
    orchestrator.run_continuous_simulation(
        duration_minutes=int(duration_hours * 60),
        interval_minutes=1,
        sample_size=sample_size
    )
    
    # Final summary
    print("\\nExtended simulation completed!")
    orchestrator.print_simulation_summary()
    
    # Final dashboard update
    # dashboard.update_metrics(orchestrator)
    # dashboard.print_dashboard()

# Uncomment the line below to run a 2-hour simulation with 100% of meters
run_extended_simulation(duration_hours=2.0, sample_percentage=1.0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Simulation Modes
# 
# - **Demo Mode**: No Azure connection - logs messages to console
# - **Production Mode**: Sends data to real Azure Event Hub
# - **Hybrid Mode**: Logs locally and sends to Event Hub
# 
# ### Performance Considerations
# 
# - **Sample Size**: Use smaller samples for testing, full dataset for production
# - **Batch Size**: Adjust EVENT_HUB_CONFIG["batch_size"] based on Event Hub limits
# - **Interval**: 15-minute intervals are typical for AMI systems
# - **Duration**: Start with short durations for testing
# 
# ### Data Characteristics
# 
# The simulation generates realistic AMI data with:
# - Seasonal and daily load patterns
# - Realistic power quality metrics
# - Geographic clustering of outages
# - Battery management for wireless meters
# - Proper failure correlation and recovery


# CELL ********************

# Final Statistics and Cleanup
print("AMI Telemetry Simulation Summary")
print("=" * 50)

# Print final statistics
print(f"Meters configured: {meters_df.count():,}")
print(f"Substations: {substations_df.count()}")
print(f"Feeders: {feeders_df.count()}")
print(f"Transformers: {transformers_df.count()}")

# Show meter distribution
print("\\nMeter Distribution:")
meter_distribution = meters_df.groupBy("meter_type").count().collect()
for row in meter_distribution:
    print(f"  {row['meter_type']}: {row['count']:,}")

# Show network distribution
print("\\nNetwork Distribution:")
feeder_distribution = meters_df.groupBy("feeder_id").count().orderBy("count", ascending=False).limit(5).collect()
print("  Top 5 feeders by meter count:")
for row in feeder_distribution:
    print(f"    {row['feeder_id']}: {row['count']} meters")

# Event Hub final stats
eh_final_stats = event_hub_client.get_stats()
print(f"\\nEvent Hub Statistics:")
print(f"  Messages sent: {eh_final_stats['total_messages_sent']:,}")
print(f"  Errors: {eh_final_stats['total_errors']}")
print(f"  Success rate: {eh_final_stats['success_rate']:.2%}")

# Cleanup resources
print("\\nCleaning up resources...")
event_hub_client.close()

# Cache DataFrames for potential reuse
meters_df.cache()
feeders_df.cache()
transformers_df.cache()
substations_df.cache()

print("\\nSimulation setup complete!")
print("All components are ready for AMI telemetry simulation.")
print("\\nTo run simulations:")
print("1. Use orchestrator.run_simulation_cycle() for single cycles")
print("2. Use orchestrator.run_continuous_simulation() for extended runs")
print("3. Monitor with dashboard.print_dashboard()")
print("4. Validate data with validator.validate_batch()")

# Optional: Stop Spark session (uncomment if needed)
# spark.stop()
# print("Spark session stopped.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
