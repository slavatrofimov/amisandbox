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

# # Thunderstorm System Simulation
# 
# This notebook generates realistic GeoJSON polygons representing a severe thunderstorm system passing over a city. The simulation creates multiple intensity zones (light rain in green, moderate rain in yellow, heavy rain in orange, and extra heavy rain in red) that move across the city over 3 minutes and then dissipate over 1 minute.
# 
# ## Features:
# - Realistic storm polygon generation with irregular shapes
# - Multiple intensity zones with nested polygons
# - Smooth transitions between animation frames
# - Geographic accuracy for city boundaries
# - Azure Event Hub integration for real-time data streaming
# - Configurable storm parameters and timing
# 
# ## Timeline:
# - **Total Duration**: 2 minutes (120 seconds)
# - **Storm Progression**: 3 minutes (90 seconds) 
# - **Storm Dissipation**: 1 minute (30 seconds)
# - **Frame Rate**: Every 5 seconds (24 total frames)

# MARKDOWN ********************

# ## 1. Import Required Libraries
# 
# Import necessary libraries for storm simulation, GeoJSON generation, and Event Hub integration.

# CELL ********************

!pip install shapely geojson azure-eventhub matplotlib numpy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import time
import math
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

# Geospatial libraries
import geojson
from shapely.geometry import Polygon, Point
from shapely.ops import unary_union
import numpy as np

# Visualization (optional, for debugging)
import matplotlib.pyplot as plt

# Azure Event Hub
try:
    from azure.eventhub import EventHubProducerClient, EventData
    AZURE_AVAILABLE = True
    print("Azure Event Hub SDK available")
except ImportError:
    print("Azure Event Hub SDK not available. Install with: pip install azure-eventhub")
    AZURE_AVAILABLE = False

# Set random seed for reproducible results
#random.seed(42)
#np.random.seed(42)

print("All libraries imported successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Define City Geographic Boundaries
# 
# #Set up City's geographic boundaries and coordinate system for accurate storm positioning.

# CELL ********************

# City center
CITY_CENTER = {
    'lat': 39.76838,
    'lon': -86.15804
}

CITY_BOUNDS_NORTH = CITY_CENTER['lat'] + .15
CITY_BOUNDS_SOUTH = CITY_CENTER['lat'] - 0.25
CITY_BOUNDS_EAST = CITY_CENTER['lon'] + .25
CITY_BOUNDS_WEST = CITY_CENTER['lon'] - 0.3

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

class CityCoordinates:
    """Helper class for working with City geographic coordinates"""
    
    @staticmethod
    def is_within_bounds(lat: float, lon: float) -> bool:
        """Check if coordinates are within City boundaries"""
        return (CITY_BOUNDS['south'] <= lat <= CITY_BOUNDS['north'] and
                CITY_BOUNDS['west'] <= lon <= CITY_BOUNDS['east'])
    
    @staticmethod
    def get_city_polygon() -> Polygon:
        """Get a simplified polygon representing City boundaries"""
        # Simplified rectangular boundary for the simulation
        return Polygon([
            (CITY_BOUNDS['west'], CITY_BOUNDS['south']),
            (CITY_BOUNDS['east'], CITY_BOUNDS['south']),
            (CITY_BOUNDS['east'], CITY_BOUNDS['north']),
            (CITY_BOUNDS['west'], CITY_BOUNDS['north'])
        ])
    
    @staticmethod
    def get_random_point_in_city() -> Tuple[float, float]:
        """Generate a random point within City boundaries"""
        lat = random.uniform(CITY_BOUNDS['south'], CITY_BOUNDS['north'])
        lon = random.uniform(CITY_BOUNDS['west'], CITY_BOUNDS['east'])
        return lat, lon

print(f"City boundaries defined:")
print(f"  North: {CITY_BOUNDS['north']}")
print(f"  South: {CITY_BOUNDS['south']}")
print(f"  East: {CITY_BOUNDS['east']}")
print(f"  West: {CITY_BOUNDS['west']}")
print(f"  City dimensions: {CITY_WIDTH:.3f}° x {CITY_HEIGHT:.3f}°")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Create Storm System Parameters
# 
# Define storm characteristics including size, intensity levels, movement patterns, and timing.

# CELL ********************

# Storm system configuration
STORM_CONFIG = {
    # Timing parameters
    'total_duration_seconds': 140,  
    'progression_duration_seconds': 120,  
    'dissipation_duration_seconds': 20,   
    'frame_interval_seconds': 2,         
    
    # Storm size parameters (in degrees)
    'initial_storm_width': 0.005,    # Initial storm width
    'initial_storm_height': 0.01,   # Initial storm height
    'max_storm_width': 0.16,        # Maximum storm width
    'max_storm_height': 0.26,       # Maximum storm height
    
    # Storm movement parameters
    'movement_direction': 'east',    # Primary direction (west to east)
    'movement_speed_deg_per_sec': CITY_WIDTH / 60,  # cross city during progression duration
    'wind_variation_degrees': 50,    # Wind direction variation
    
    # Rain intensity zones (nested polygons)
    'intensity_zones': {
        'extra_heavy': {'color': '#FF0000', 'size_factor': 0.15, 'name': 'Extra Heavy Rain'},
        'heavy': {'color': '#FF8C00', 'size_factor': 0.3, 'name': 'Heavy Rain'},
        'moderate': {'color': '#FFD700', 'size_factor': 0.6, 'name': 'Moderate Rain'},
        'light': {'color': '#32CD32', 'size_factor': 1.0, 'name': 'Light Rain'}
    }
}

# Calculate total number of frames
TOTAL_FRAMES = STORM_CONFIG['total_duration_seconds'] // STORM_CONFIG['frame_interval_seconds']
PROGRESSION_FRAMES = STORM_CONFIG['progression_duration_seconds'] // STORM_CONFIG['frame_interval_seconds']
DISSIPATION_FRAMES = STORM_CONFIG['dissipation_duration_seconds'] // STORM_CONFIG['frame_interval_seconds']

# Storm starting position (west of City)
STORM_START_POSITION = {
    'lat': CITY_CENTER['lat'] + random.uniform(-0.15, 0.15),  # Slight north/south variation
    'lon': CITY_BOUNDS['west'] - 0.35  # Start west of the city
}  # Start west of the city

print(f"Storm system parameters:")
print(f"  Total frames: {TOTAL_FRAMES}")
print(f"  Progression frames: {PROGRESSION_FRAMES}")
print(f"  Dissipation frames: {DISSIPATION_FRAMES}")
print(f"  Movement speed: {STORM_CONFIG['movement_speed_deg_per_sec']:.6f} deg/sec")
print(f"  Starting position: {STORM_START_POSITION['lat']:.4f}, {STORM_START_POSITION['lon']:.4f}")
print(f"  Intensity zones: {list(STORM_CONFIG['intensity_zones'].keys())}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Generate Base Storm Shapes
# 
# Create functions to generate irregular polygon shapes that resemble natural storm cells. The storm structure places heavier rainfall toward the front (leading edge) of the storm system, which is more meteorologically accurate.

# CELL ********************

class StormShapeGenerator:
    """Generate realistic irregular storm polygon shapes"""
    
    @staticmethod
    def generate_irregular_polygon(center_lat: float, center_lon: float, 
                                 width: float, height: float, 
                                 num_points: int = 16, 
                                 irregularity: float = 0.3) -> Polygon:
        """
        Generate an irregular polygon that resembles a natural storm cell with smooth curves
        
        Args:
            center_lat, center_lon: Center coordinates
            width, height: Approximate dimensions
            num_points: Number of vertices (kept moderate for smooth curves)
            irregularity: How irregular the shape is (0-1)
        """
        # Generate base points with smooth variation
        base_points = []
        angle_step = 2 * math.pi / num_points
        
        # Create smooth radius variations using sine waves
        radius_variations = []
        for i in range(num_points):
            # Use multiple sine waves for natural-looking variation
            variation = (
                0.7 * math.sin(i * 2 * math.pi / num_points * 3) +  # Primary variation
                0.2 * math.sin(i * 2 * math.pi / num_points * 7) +  # Secondary variation
                0.1 * math.sin(i * 2 * math.pi / num_points * 11)   # Fine detail
            )
            radius_variations.append(variation * irregularity)
        
        for i in range(num_points):
            # Base angle for this point
            angle = i * angle_step
            
            # Calculate base radius (elliptical)
            radius_x = width / 2
            radius_y = height / 2
            
            # Elliptical radius calculation
            radius = (radius_x * radius_y) / math.sqrt(
                (radius_y * math.cos(angle))**2 + (radius_x * math.sin(angle))**2
            )
            
            # Apply smooth radius variation
            radius *= (1 + radius_variations[i])
            
            # Convert to lat/lon coordinates
            lat_offset = radius * math.sin(angle)
            lon_offset = radius * math.cos(angle)
            
            lat = center_lat + lat_offset
            lon = center_lon + lon_offset
            
            base_points.append((lon, lat))  # GeoJSON uses lon, lat order
        
        # Create smooth interpolated points for even smoother curves
        smooth_points = []
        interpolation_factor = 2  # Add 1 interpolated point between each base point
        
        for i in range(len(base_points)):
            # Add the current point
            smooth_points.append(base_points[i])
            
            # Add interpolated point(s) to next point
            next_i = (i + 1) % len(base_points)
            current_point = base_points[i]
            next_point = base_points[next_i]
            
            for j in range(1, interpolation_factor + 1):
                t = j / (interpolation_factor + 1)
                # Smooth interpolation
                interp_lon = current_point[0] + t * (next_point[0] - current_point[0])
                interp_lat = current_point[1] + t * (next_point[1] - current_point[1])
                smooth_points.append((interp_lon, interp_lat))
        
        return Polygon(smooth_points)
    
    @staticmethod
    def generate_storm_cell(center_lat: float, center_lon: float, 
                          width: float, height: float,
                          intensity_factor: float = 1.0) -> Polygon:
        """
        Generate a storm cell with characteristics typical of thunderstorms
        
        Args:
            center_lat, center_lon: Center coordinates
            width, height: Base dimensions
            intensity_factor: Intensity multiplier (affects irregularity)
        """
        # More intense storms tend to be more irregular but keep it moderate for smooth curves
        irregularity = 0.15 + (intensity_factor * 0.05)
        
        # Use moderate number of points for smooth, natural curves
        # Too many points create jagged edges, too few create obvious polygons
        num_points = max(12, min(20, int(10 + intensity_factor * 3)))
        
        return StormShapeGenerator.generate_irregular_polygon(
            center_lat, center_lon, width, height, num_points, irregularity
        )
    
    @staticmethod
    def generate_specialized_storm_cell(center_lat: float, center_lon: float, 
                                      width: float, height: float,
                                      irregularity: float, num_points: int,
                                      variation_pattern: str) -> Polygon:
        """
        Generate a storm cell with specialized characteristics for different intensity zones
        
        Args:
            center_lat, center_lon: Center coordinates
            width, height: Dimensions
            irregularity: Irregularity factor
            num_points: Number of vertices
            variation_pattern: Type of variation ('smooth', 'medium', 'active', 'intense')
        """
        # Generate base points with pattern-specific variation
        base_points = []
        angle_step = 2 * math.pi / num_points
        
        # Create pattern-specific radius variations
        radius_variations = []
        for i in range(num_points):
            if variation_pattern == 'smooth':
                # Gentle, flowing variations - light rain
                variation = (
                    0.8 * math.sin(i * 2 * math.pi / num_points * 4) +
                    0.2 * math.sin(i * 2 * math.pi / num_points * 7)
                )
            elif variation_pattern == 'medium':
                # Moderate variations with some detail - moderate rain
                variation = (
                    0.6 * math.sin(i * 2 * math.pi / num_points * 3) +
                    0.3 * math.sin(i * 2 * math.pi / num_points * 7) +
                    0.1 * math.sin(i * 2 * math.pi / num_points * 13)
                )
            elif variation_pattern == 'active':
                # More complex patterns - heavy rain
                variation = (
                    0.5 * math.sin(i * 2 * math.pi / num_points * 4) +
                    0.3 * math.sin(i * 2 * math.pi / num_points * 8) +
                    0.1 * math.sin(i * 2 * math.pi / num_points * 10)
                )
            else:  # 'intense'
                # Very complex, turbulent patterns - extra heavy rain
                variation = (
                    0.3 * math.sin(i * 2 * math.pi / num_points * 3) +
                    0.2 * math.sin(i * 2 * math.pi / num_points * 8) +
                    0.1 * math.sin(i * 2 * math.pi / num_points * 10) +
                    0.05 * math.sin(i * 2 * math.pi / num_points * 12)
                )
            
            radius_variations.append(variation * irregularity)
        
        for i in range(num_points):
            # Base angle for this point
            angle = i * angle_step
            
            # Calculate base radius (elliptical)
            radius_x = width / 2
            radius_y = height / 2
            
            # Elliptical radius calculation
            radius = (radius_x * radius_y) / math.sqrt(
                (radius_y * math.cos(angle))**2 + (radius_x * math.sin(angle))**2
            )
            
            # Apply pattern-specific radius variation
            radius *= (1 + radius_variations[i])
            
            # Convert to lat/lon coordinates
            lat_offset = radius * math.sin(angle)
            lon_offset = radius * math.cos(angle)
            
            lat = center_lat + lat_offset
            lon = center_lon + lon_offset
            
            base_points.append((lon, lat))  # GeoJSON uses lon, lat order
        
        # Create smooth interpolated points, with different interpolation based on pattern
        smooth_points = []
        if variation_pattern in ['smooth', 'medium']:
            interpolation_factor = 3  # More interpolation for smoother zones
        else:
            interpolation_factor = 2  # Less interpolation for more angular intense zones
        
        for i in range(len(base_points)):
            # Add the current point
            smooth_points.append(base_points[i])
            
            # Add interpolated point(s) to next point
            if interpolation_factor > 0:
                next_i = (i + 1) % len(base_points)
                current_point = base_points[i]
                next_point = base_points[next_i]
                
                for j in range(1, interpolation_factor + 1):
                    t = j / (interpolation_factor + 1)
                    # Smooth interpolation
                    interp_lon = current_point[0] + t * (next_point[0] - current_point[0])
                    interp_lat = current_point[1] + t * (next_point[1] - current_point[1])
                    smooth_points.append((interp_lon, interp_lat))
        
        return Polygon(smooth_points)
    
    @staticmethod
    def create_nested_intensity_zones(center_lat: float, center_lon: float,
                                    base_width: float, base_height: float,
                                    intensity_factor: float = 1.0) -> Dict[str, Polygon]:
        """
        Create nested polygons for different rain intensity zones with heavy rain at the front
        Each intensity zone has distinct visual characteristics
        
        Returns dict with intensity level as key and polygon as value
        """
        zones = {}
        
        # Define the offset multipliers for each intensity zone
        # Higher intensity zones are positioned more toward the front (east) of the storm
        zone_offsets = {
            'light': 0.0,        # Light rain stays at storm center
            'moderate': 0.1,    # Moderate rain slightly forward
            'heavy': 0.2,       # Heavy rain toward the front
            'extra_heavy': 0.25  # Extra heavy rain at the leading edge
        }
        
        # Define unique characteristics for each intensity zone
        zone_characteristics = {
            'light': {
                'shape_factor': 0.95,     # More circular/elliptical
                'irregularity_boost': 0.07,  # Smooth edges
                'aspect_ratio': 1.1,     # Slightly elongated
                'num_points_base': 37,   # Moderate detail
                'variation_pattern': 'smooth'  # Gentle variations
            },
            'moderate': {
                'shape_factor': 0.9,     # Slightly more compact
                'irregularity_boost': 0.1,  # A bit more irregular
                'aspect_ratio': 1.2,     # More elongated
                'num_points_base': 45,   # More detail
                'variation_pattern': 'medium'  # Medium variations
            },
            'heavy': {
                'shape_factor': 0.85,     # More compact and intense
                'irregularity_boost': 0.12,  # More irregular
                'aspect_ratio': 1.3,     # Quite elongated
                'num_points_base': 50,   # Higher detail
                'variation_pattern': 'active'  # More active variations
            },
            'extra_heavy': {
                'shape_factor': 0.8,     # Very compact core
                'irregularity_boost': 0.15,  # Most irregular
                'aspect_ratio': 1.4,     # Very elongated
                'num_points_base': 50,   # Highest detail
                'variation_pattern': 'intense'  # Most intense variations
            }
        }
        
        for intensity, config in STORM_CONFIG['intensity_zones'].items():
            zone_width = base_width * config['size_factor']
            zone_height = base_height * config['size_factor']
            
            # Get unique characteristics for this zone
            char = zone_characteristics[intensity]
            
            # Apply shape factor to make zones more distinct
            zone_width *= char['shape_factor']
            zone_height *= char['shape_factor'] * char['aspect_ratio']
            
            # Calculate the offset position for this intensity zone
            # Positive offset moves the zone eastward (direction of storm movement)
            offset_distance = zone_offsets[intensity] * base_width
            zone_center_lat = center_lat
            zone_center_lon = center_lon + offset_distance
            
            # Calculate unique irregularity for this zone
            base_irregularity = intensity_factor * (1.1 - config['size_factor'])
            zone_irregularity = base_irregularity + char['irregularity_boost']
            
            # Calculate number of points based on intensity characteristics
            num_points = char['num_points_base'] + int(intensity_factor * 4)
            
            # Generate the polygon with unique characteristics
            polygon = StormShapeGenerator.generate_specialized_storm_cell(
                zone_center_lat, zone_center_lon, zone_width, zone_height, 
                zone_irregularity, num_points, char['variation_pattern']
            )
            
            zones[intensity] = polygon
        
        return zones

# Test the storm shape generator
print("Testing storm shape generation...")
print("Note: Each intensity zone now has unique visual characteristics:")
print("  • Light rain: Smooth, circular, gentle variations")
print("  • Moderate rain: Slightly elongated, moderate detail")  
print("  • Heavy rain: More compact, higher irregularity, active patterns")
print("  • Extra heavy rain: Very elongated, most irregular, intense turbulent patterns")
print()

test_center_lat, test_center_lon = CITY_CENTER['lat'], CITY_CENTER['lon']
test_zones = StormShapeGenerator.create_nested_intensity_zones(
    test_center_lat, test_center_lon, 0.1, 0.08, 1.0
)

print(f"Generated {len(test_zones)} intensity zones with distinct characteristics:")
for intensity, polygon in test_zones.items():
    coords_count = len(polygon.exterior.coords)
    area = polygon.area
    # Calculate the centroid to show the offset
    centroid = polygon.centroid
    
    # Calculate bounding box to show shape differences
    minx, miny, maxx, maxy = polygon.bounds
    width_ratio = (maxx - minx) / (maxy - miny) if (maxy - miny) > 0 else 1.0
    
    print(f"  {intensity}: {coords_count} vertices, area: {area:.8f} sq degrees")
    print(f"    Center: ({centroid.y:.6f}, {centroid.x:.6f}), Aspect ratio: {width_ratio:.2f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Implement Storm Movement Functions
# 
# Develop algorithms to move storm polygons across City with realistic patterns.

# CELL ********************

class StormMovement:
    """Handle storm movement and evolution over time"""
    
    def __init__(self, start_lat: float, start_lon: float):
        self.start_lat = start_lat
        self.start_lon = start_lon
        self.current_lat = start_lat
        self.current_lon = start_lon
        
    def calculate_position(self, frame_number: int, total_frames: int) -> Tuple[float, float]:
        """
        Calculate storm position for a given frame
        
        Args:
            frame_number: Current frame (0-based)
            total_frames: Total number of frames in progression
        """
        if frame_number >= PROGRESSION_FRAMES:
            # During dissipation, storm continues moving but slower
            progression_progress = 1.0
            dissipation_frame = frame_number - PROGRESSION_FRAMES
            dissipation_progress = dissipation_frame / DISSIPATION_FRAMES
            additional_movement = dissipation_progress * 0.3  # Move 30% more during dissipation
            progress = progression_progress + additional_movement
        else:
            # During progression phase
            progress = frame_number / PROGRESSION_FRAMES
        
        # Calculate distance traveled
        total_distance = CITY_WIDTH + 0.3  # Cross city plus some extra
        distance_traveled = progress * total_distance
        
        # Add some wind variation (sinusoidal pattern)
        wind_variation = math.sin(progress * math.pi * 2) * 0.02  # Small north-south drift
        
        # Calculate new position
        self.current_lat = self.start_lat + wind_variation
        self.current_lon = self.start_lon + distance_traveled
        
        return self.current_lat, self.current_lon
    
    def calculate_storm_size(self, frame_number: int) -> Tuple[float, float]:
        """
        Calculate storm size evolution over time
        
        Returns: (width, height)
        """
        if frame_number < PROGRESSION_FRAMES:
            # Growth phase - storm grows as it approaches and peaks over city
            progress = frame_number / PROGRESSION_FRAMES
            
            # Storm grows more rapidly in the middle portion
            size_multiplier = 0.7 + 0.6 * math.sin(progress * math.pi)
            
            width = STORM_CONFIG['initial_storm_width'] + \
                   (STORM_CONFIG['max_storm_width'] - STORM_CONFIG['initial_storm_width']) * size_multiplier
            height = STORM_CONFIG['initial_storm_height'] + \
                    (STORM_CONFIG['max_storm_height'] - STORM_CONFIG['initial_storm_height']) * size_multiplier
        else:
            # Dissipation phase - storm shrinks
            dissipation_frame = frame_number - PROGRESSION_FRAMES
            dissipation_progress = dissipation_frame / DISSIPATION_FRAMES
            
            # Exponential decay for realistic dissipation
            decay_factor = math.exp(-3 * dissipation_progress)
            
            width = STORM_CONFIG['max_storm_width'] * decay_factor
            height = STORM_CONFIG['max_storm_height'] * decay_factor
        
        return width, height
    
    def calculate_intensity(self, frame_number: int) -> float:
        """
        Calculate storm intensity over time (0-1 scale)
        """
        if frame_number < PROGRESSION_FRAMES:
            # Build up to peak intensity
            progress = frame_number / PROGRESSION_FRAMES
            
            # Peak intensity when storm is over the city (around frame 9-12)
            peak_frame_ratio = 0.6  # Peak at 60% through progression
            
            if progress < peak_frame_ratio:
                # Building up
                intensity = 0.3 + 0.7 * (progress / peak_frame_ratio)
            else:
                # Slight decline after peak
                decline_progress = (progress - peak_frame_ratio) / (1 - peak_frame_ratio)
                intensity = 1.0 - 0.1 * decline_progress
                
            return min(1.0, intensity)
        else:
            # Dissipation phase
            dissipation_frame = frame_number - PROGRESSION_FRAMES
            dissipation_progress = dissipation_frame / DISSIPATION_FRAMES
            
            # Exponential intensity decay
            return 0.9 * math.exp(-2 * dissipation_progress)

# Initialize storm movement
storm_movement = StormMovement(STORM_START_POSITION['lat'], STORM_START_POSITION['lon'])

# Test storm movement calculations
print("Testing storm movement calculations...")
print("Frame | Position (lat, lon) | Size (w x h) | Intensity")
print("-" * 60)

for frame in [0, 5, 10, 15, 18, 20, 23]:
    lat, lon = storm_movement.calculate_position(frame, TOTAL_FRAMES)
    width, height = storm_movement.calculate_storm_size(frame)
    intensity = storm_movement.calculate_intensity(frame)
    
    print(f"{frame:5d} | ({lat:6.3f}, {lon:7.3f}) | {width:.3f} x {height:.3f} | {intensity:.3f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Generate GeoJSON Storm Data
# 
# Create functions to generate GeoJSON-formatted storm data for Event Hub transmission.

# CELL ********************

class StormGeoJSONGenerator:
    """Generate GeoJSON data for storm visualization"""
    
    @staticmethod
    def polygon_to_geojson(polygon: Polygon, properties: Dict) -> Dict:
        """Convert Shapely polygon to GeoJSON feature"""
        # Extract coordinates (GeoJSON expects [[[lon, lat]]] format for polygons)
        coords = [list(polygon.exterior.coords)]
        
        return {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": coords
            },
            "properties": properties
        }
    
    @staticmethod
    def generate_storm_frame(frame_number: int, timestamp: datetime) -> Dict:
        """
        Generate a complete storm frame with all intensity zones
        
        Args:
            frame_number: Current frame number (0-based)
            timestamp: Timestamp for this frame
            
        Returns:
            Complete GeoJSON FeatureCollection for this frame
        """
        # Calculate storm position, size, and intensity
        lat, lon = storm_movement.calculate_position(frame_number, TOTAL_FRAMES)
        width, height = storm_movement.calculate_storm_size(frame_number)
        intensity = storm_movement.calculate_intensity(frame_number)
        
        # Generate intensity zone polygons
        storm_zones = StormShapeGenerator.create_nested_intensity_zones(
            lat, lon, width, height, intensity
        )
        
        # Create GeoJSON features for each zone
        features = []
        
        for zone_name, polygon in storm_zones.items():
            zone_config = STORM_CONFIG['intensity_zones'][zone_name]
            
            # Calculate rainfall rate based on intensity and zone
            base_rainfall_rates = {
                'light': 2.5,      # mm/hr
                'moderate': 7.5,   # mm/hr  
                'heavy': 25.0,     # mm/hr
                'extra_heavy': 50.0 # mm/hr
            }
            
            rainfall_rate = base_rainfall_rates[zone_name] * intensity
            
            properties = {
                "intensity_level": zone_name,
                "color": zone_config['color'],
                "name": zone_config['name'],
                "rainfall_rate_mm_per_hour": round(rainfall_rate, 1),
                "storm_intensity": round(intensity, 3),
                "frame_number": frame_number,
                "timestamp": timestamp.isoformat(),
                "center_lat": round(lat, 6),
                "center_lon": round(lon, 6),
                "storm_width_degrees": round(width, 6),
                "storm_height_degrees": round(height, 6)
            }
            
            feature = StormGeoJSONGenerator.polygon_to_geojson(polygon, properties)
            features.append(feature)
        
        # Create complete GeoJSON FeatureCollection
        geojson_data = {
            "type": "FeatureCollection",
            "properties": {
                "storm_system_id": f"detroit_storm_{timestamp.strftime('%Y%m%d_%H%M%S')}",
                "frame_number": frame_number,
                "total_frames": TOTAL_FRAMES,
                "timestamp": timestamp.isoformat(),
                "location": "City, MI",
                "storm_phase": "progression" if frame_number < PROGRESSION_FRAMES else "dissipation",
                "storm_center": {"lat": lat, "lon": lon},
                "overall_intensity": round(intensity, 3)
            },
            "features": features
        }
        
        return geojson_data
    
    @staticmethod
    def validate_geojson(geojson_data: Dict) -> bool:
        """Validate GeoJSON structure"""
        try:
            # Basic structure validation
            if geojson_data.get("type") != "FeatureCollection":
                return False
            
            if "features" not in geojson_data:
                return False
            
            # Validate each feature
            for feature in geojson_data["features"]:
                if feature.get("type") != "Feature":
                    return False
                
                if "geometry" not in feature or "properties" not in feature:
                    return False
                
                geometry = feature["geometry"]
                if geometry.get("type") != "Polygon":
                    return False
                
                if "coordinates" not in geometry:
                    return False
            
            return True
            
        except Exception as e:
            print(f"GeoJSON validation error: {e}")
            return False

# Test GeoJSON generation
print("Testing GeoJSON generation...")
test_timestamp = datetime.now()
test_frame = StormGeoJSONGenerator.generate_storm_frame(10, test_timestamp)

print(f"Generated GeoJSON frame:")
print(f"  Type: {test_frame['type']}")
print(f"  Features: {len(test_frame['features'])}")
print(f"  Storm phase: {test_frame['properties']['storm_phase']}")
print(f"  Overall intensity: {test_frame['properties']['overall_intensity']}")

# Validate the generated GeoJSON
is_valid = StormGeoJSONGenerator.validate_geojson(test_frame)
print(f"  GeoJSON valid: {is_valid}")

# Show sample feature
if test_frame['features']:
    sample_feature = test_frame['features'][0]
    print(f"\nSample feature ({sample_feature['properties']['intensity_level']}):")
    print(f"  Color: {sample_feature['properties']['color']}")
    print(f"  Rainfall rate: {sample_feature['properties']['rainfall_rate_mm_per_hour']} mm/hr")
    print(f"  Coordinates: {len(sample_feature['geometry']['coordinates'][0])} points")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Setup Event Hub Connection
# 
# Configure Azure Event Hub connection for sending storm data in real-time.

# CELL ********************

AZURE_EVENTHUB_CONNECTION_STRING = spark.sql("SELECT value FROM ReferenceDataLH.secrets WHERE name = 'Weather-EH-ConnectionString'").first()[0]

# Event Hub Configuration
EVENT_HUB_CONFIG = {
    # Replace with your actual Event Hub connection string
    'connection_string': AZURE_EVENTHUB_CONNECTION_STRING,
    'event_hub_name': 'es_a860b47f-5979-4704-878f-915756a985d0',
    'consumer_group': '$Default'
}

class EventHubStormSender:
    """Handle sending storm data to Azure Event Hub"""
    
    def __init__(self, connection_string: str, event_hub_name: str):
        self.connection_string = connection_string
        self.event_hub_name = event_hub_name
        self.client = None
        
        if AZURE_AVAILABLE:
            try:
                self.client = EventHubProducerClient.from_connection_string(
                    connection_string, eventhub_name=event_hub_name
                )
                print(f"Event Hub producer client initialized for: {event_hub_name}")
            except Exception as e:
                print(f"Failed to initialize Event Hub client: {e}")
                self.client = None
        else:
            print("Azure Event Hub SDK not available - will simulate sending")
    
    def send_storm_event(self, geojson_data: Dict, frame_number: int) -> bool:
        """
        Send storm GeoJSON data to Event Hub
        
        Args:
            geojson_data: The GeoJSON storm data
            frame_number: Current frame number
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert GeoJSON to JSON string
            json_message = json.dumps(geojson_data, separators=(',', ':'))
            
            if self.client:
                # Send to actual Event Hub
                event_data = EventData(json_message)
                
                # Add custom properties for routing/filtering
                event_data.properties = {
                    'event_type': 'storm_frame',
                    'frame_number': frame_number,
                    'location': 'detroit',
                    'data_format': 'geojson'
                }
                
                with self.client:
                    event_data_batch = self.client.create_batch()
                    event_data_batch.add(event_data)
                    self.client.send_batch(event_data_batch)
                
                print(f"✓ Sent frame {frame_number} to Event Hub ({len(json_message)} bytes)")
                return True
            else:
                # Simulate sending (for testing without actual Event Hub)
                print(f"✓ [SIMULATED] Sent frame {frame_number} ({len(json_message)} bytes)")
                return True
                
        except Exception as e:
            print(f"✗ Failed to send frame {frame_number}: {e}")
            return False
    
    def close(self):
        """Close the Event Hub connection"""
        if self.client:
            self.client.close()
            print("Event Hub client closed")

# Configuration setup
print("Event Hub Configuration:")
print("=" * 50)
print("To use this notebook with a real Event Hub:")
print("1. Create an Azure Event Hub namespace")
print("2. Create an Event Hub named 'storm-events'")
print("3. Get the connection string from Azure portal")
print("4. Update the EVENT_HUB_CONFIG above")
print("\\nCurrent configuration:")
print(f"  Event Hub Name: {EVENT_HUB_CONFIG['event_hub_name']}")

# Check if we have a real connection string
if "your-namespace" in EVENT_HUB_CONFIG['connection_string']:
    print("  Status: Demo mode (simulated sending)")
    USE_REAL_EVENT_HUB = False
else:
    print("  Status: Real Event Hub configured")
    USE_REAL_EVENT_HUB = True

# Initialize Event Hub sender
event_hub_sender = EventHubStormSender(
    EVENT_HUB_CONFIG['connection_string'],
    EVENT_HUB_CONFIG['event_hub_name']
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Create Storm Animation Loop
# 
# Run the main simulation that generates and sends storm frames over the 4-minute timeline.

# CELL ********************

from typing import List, Dict

def run_storm_simulation(send_to_event_hub: bool = True, 
                        save_to_files: bool = False,
                        accelerated: bool = False) -> List[Dict]:
    """
    Run the complete storm simulation
    
    Args:
        send_to_event_hub: Whether to send data to Event Hub
        save_to_files: Whether to save GeoJSON files locally
        accelerated: If True, run faster for testing (1 second intervals)
        
    Returns:
        List of all generated GeoJSON frames
    """
    print("🌩️  Starting City Storm Simulation")
    print("=" * 60)
    print(f"Total duration: {STORM_CONFIG['total_duration_seconds']} seconds")
    print(f"Total frames: {TOTAL_FRAMES}")
    print(f"Frame interval: {STORM_CONFIG['frame_interval_seconds']} seconds")
    print(f"Send to Event Hub: {send_to_event_hub}")
    print(f"Save to files: {save_to_files}")
    print(f"Accelerated mode: {accelerated}")
    print("-" * 60)
    
    start_time = datetime.now()
    generated_frames = []
    successful_sends = 0
    
    for frame_number in range(TOTAL_FRAMES):
        frame_start_time = time.time()
        
        # Calculate frame timestamp
        frame_timestamp = start_time + timedelta(
            seconds=frame_number * STORM_CONFIG['frame_interval_seconds']
        )
        
        try:
            # Generate storm frame
            geojson_frame = StormGeoJSONGenerator.generate_storm_frame(
                frame_number, frame_timestamp
            )
            
            generated_frames.append(geojson_frame)
            
            # Display progress
            storm_phase = geojson_frame['properties']['storm_phase']
            intensity = geojson_frame['properties']['overall_intensity']
            center = geojson_frame['properties']['storm_center']
            
            progress_percent = (frame_number / TOTAL_FRAMES) * 100
            
            print(f"Frame {frame_number:2d}/{TOTAL_FRAMES} ({progress_percent:5.1f}%) | " +
                  f"{storm_phase:12s} | Intensity: {intensity:.3f} | " +
                  f"Center: ({center['lat']:.3f}, {center['lon']:.3f})")
            
            # Send to Event Hub if requested
            if send_to_event_hub:
                success = event_hub_sender.send_storm_event(geojson_frame, frame_number)
                if success:
                    successful_sends += 1
            
            # Save to file if requested
            if save_to_files:
                filename = f"storm_frame_{frame_number:02d}_{frame_timestamp.strftime('%H%M%S')}.geojson"
                with open(filename, 'w') as f:
                    json.dump(geojson_frame, f, indent=2)
            
            # Wait for next frame (unless accelerated)
            if not accelerated:
                frame_duration = time.time() - frame_start_time
                sleep_time = STORM_CONFIG['frame_interval_seconds'] - frame_duration
                if sleep_time > 0:
                    time.sleep(sleep_time)
            else:
                # Accelerated mode - just 1 second between frames
                time.sleep(1)
                
        except Exception as e:
            print(f"✗ Error generating frame {frame_number}: {e}")
            continue
    
    # Simulation complete
    end_time = datetime.now()
    actual_duration = (end_time - start_time).total_seconds()
    
    print("=" * 60)
    print("🌩️  Storm Simulation Complete!")
    print(f"Generated frames: {len(generated_frames)}")
    print(f"Successful sends: {successful_sends}")
    print(f"Actual duration: {actual_duration:.1f} seconds")
    print(f"Start time: {start_time.strftime('%H:%M:%S')}")
    print(f"End time: {end_time.strftime('%H:%M:%S')}")
    
    return generated_frames

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Run Full Storm Simulation
# 
# Execute the complete 4-minute storm simulation with real-time Event Hub transmission.

# CELL ********************

# Run the full simulation
print("🚀 Ready to run the full storm simulation!")
print()
print("This will:")
print("• Generate 24 storm frames over 4 minutes")
print("• Send each frame to Event Hub every 10 seconds")
print("• Simulate realistic storm progression and dissipation")
print("• Include 4 intensity zones (light, moderate, heavy, extra heavy rain)")
print()

# Uncomment the line below to run the full simulation
# WARNING: This will take 4 minutes in real-time!

full_simulation_frames = run_storm_simulation (
     send_to_event_hub=True,   # Send to Event Hub
     save_to_files=False,       # Also save as local files
     accelerated=False         # Real-time simulation
 )

print("To run the full simulation, uncomment the code above.")
print("For testing, you can run the accelerated version:")
print()

# Run accelerated version for testing
print("Running accelerated simulation for demonstration...")
#test_frames = run_storm_simulation(
#    send_to_event_hub=True,   # Send to Event Hub (simulated if no real connection)
#    save_to_files=False,      # Don't save files for test
#    accelerated=True          # Fast mode - 1 second per frame
#)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Analyze and Visualize Storm Data
# 
# Examine the generated storm data and create simple visualizations.

# CELL ********************

from typing import List, Dict

def analyze_storm_data(frames: List[Dict]) -> Dict:
    """Analyze the generated storm data"""
    
    if not frames:
        return {"error": "No frames to analyze"}
    
    analysis = {
        "total_frames": len(frames),
        "storm_progression": {},
        "intensity_analysis": {},
        "geographic_coverage": {}
    }
    
    # Analyze storm progression
    storm_centers = []
    intensities = []
    storm_sizes = []
    
    for frame in frames:
        props = frame['properties']
        storm_centers.append((props['storm_center']['lat'], props['storm_center']['lon']))
        intensities.append(props['overall_intensity'])
        
        # Calculate total storm area (sum of all zones)
        total_area = sum(
            Polygon(feature['geometry']['coordinates'][0]).area 
            for feature in frame['features']
        )
        storm_sizes.append(total_area)
    
    analysis['storm_progression'] = {
        "path_length_degrees": len(storm_centers),
        "max_intensity": max(intensities),
        "min_intensity": min(intensities),
        "avg_intensity": sum(intensities) / len(intensities),
        "max_storm_size": max(storm_sizes),
        "min_storm_size": min(storm_sizes)
    }
    
    # Analyze intensity zones
    zone_stats = {}
    for intensity_level in STORM_CONFIG['intensity_zones'].keys():
        zone_stats[intensity_level] = {
            "frames_present": 0,
            "total_rainfall": 0,
            "max_rainfall_rate": 0
        }
    
    for frame in frames:
        for feature in frame['features']:
            intensity_level = feature['properties']['intensity_level']
            rainfall_rate = feature['properties']['rainfall_rate_mm_per_hour']
            
            zone_stats[intensity_level]["frames_present"] += 1
            zone_stats[intensity_level]["total_rainfall"] += rainfall_rate
            zone_stats[intensity_level]["max_rainfall_rate"] = max(
                zone_stats[intensity_level]["max_rainfall_rate"], rainfall_rate
            )
    
    analysis['intensity_analysis'] = zone_stats
    
    # Geographic coverage
    all_lats = [center[0] for center in storm_centers]
    all_lons = [center[1] for center in storm_centers]
    
    analysis['geographic_coverage'] = {
        "lat_range": [min(all_lats), max(all_lats)],
        "lon_range": [min(all_lons), max(all_lons)],
        "path_distance_degrees": max(all_lons) - min(all_lons)
    }
    
    return analysis

def visualize_storm_progression(frames: List[Dict]):
    """Create simple visualizations of storm progression"""
    
    if not frames:
        print("No frames to visualize")
        return
    
    # Extract data for plotting
    frame_numbers = list(range(len(frames)))
    intensities = [frame['properties']['overall_intensity'] for frame in frames]
    lats = [frame['properties']['storm_center']['lat'] for frame in frames]
    lons = [frame['properties']['storm_center']['lon'] for frame in frames]
    
    # Create subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 10))
    
    # Plot 1: Storm intensity over time
    ax1.plot(frame_numbers, intensities, 'b-', linewidth=2, marker='o')
    ax1.set_title('Storm Intensity Over Time')
    ax1.set_xlabel('Frame Number')
    ax1.set_ylabel('Intensity (0-1)')
    ax1.grid(True, alpha=0.3)
    ax1.axvline(x=PROGRESSION_FRAMES, color='r', linestyle='--', alpha=0.7, label='Dissipation starts')
    ax1.legend()
    
    # Plot 2: Storm path (lat/lon)
    ax2.plot(lons, lats, 'g-', linewidth=2, marker='s', markersize=4)
    ax2.set_title('Storm Path Over City')
    ax2.set_xlabel('Longitude')
    ax2.set_ylabel('Latitude')
    ax2.grid(True, alpha=0.3)
    
    # Add City boundaries
    ax2.axhline(y=CITY_BOUNDS['north'], color='k', linestyle='-', alpha=0.3)
    ax2.axhline(y=CITY_BOUNDS['south'], color='k', linestyle='-', alpha=0.3)
    ax2.axvline(x=CITY_BOUNDS['east'], color='k', linestyle='-', alpha=0.3)
    ax2.axvline(x=CITY_BOUNDS['west'], color='k', linestyle='-', alpha=0.3)
    
    # Plot 3: Longitude progression (movement)
    ax3.plot(frame_numbers, lons, 'r-', linewidth=2, marker='^')
    ax3.set_title('Storm Movement (West to East)')
    ax3.set_xlabel('Frame Number')
    ax3.set_ylabel('Longitude')
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Rainfall rates by intensity zone
    rainfall_data = {zone: [] for zone in STORM_CONFIG['intensity_zones'].keys()}
    
    for frame in frames:
        frame_rainfall = {zone: 0 for zone in STORM_CONFIG['intensity_zones'].keys()}
        for feature in frame['features']:
            zone = feature['properties']['intensity_level']
            rainfall = feature['properties']['rainfall_rate_mm_per_hour']
            frame_rainfall[zone] = rainfall
        
        for zone in rainfall_data:
            rainfall_data[zone].append(frame_rainfall[zone])
    
    for zone, rates in rainfall_data.items():
        color = STORM_CONFIG['intensity_zones'][zone]['color']
        ax4.plot(frame_numbers, rates, color=color, linewidth=2, label=zone, marker='o', markersize=3)
    
    ax4.set_title('Rainfall Rates by Intensity Zone')
    ax4.set_xlabel('Frame Number')
    ax4.set_ylabel('Rainfall Rate (mm/hr)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

# Analyze the generated storm data
if 'test_frames' in locals() and test_frames:
    print("📊 Analyzing storm simulation data...")
    
    analysis_results = analyze_storm_data(test_frames)
    
    print("\\nStorm Analysis Results:")
    print("=" * 40)
    print(f"Total frames generated: {analysis_results['total_frames']}")
    print(f"Maximum intensity: {analysis_results['storm_progression']['max_intensity']:.3f}")
    print(f"Average intensity: {analysis_results['storm_progression']['avg_intensity']:.3f}")
    print(f"Path distance: {analysis_results['geographic_coverage']['path_distance_degrees']:.3f} degrees")
    
    print("\\nIntensity Zone Statistics:")
    for zone, stats in analysis_results['intensity_analysis'].items():
        print(f"  {zone}: Present in {stats['frames_present']} frames, " +
              f"Max rainfall: {stats['max_rainfall_rate']:.1f} mm/hr")
    
    # Create visualizations
    print("\\n📈 Generating visualizations...")
    visualize_storm_progression(test_frames)
    
else:
    print("No storm data available for analysis. Run the simulation first.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary and Next Steps
# 
# This notebook successfully simulates a realistic thunderstorm system passing over City, MI with the following features:
# 
# ### ✅ Completed Features:
# - **Realistic Storm Shapes**: Generated irregular polygons using mathematical curves and randomization
# - **Multiple Intensity Zones**: Four nested rain intensity levels (light, moderate, heavy, extra heavy)
# - **Geographic Accuracy**: Used actual City boundaries and coordinates
# - **Smooth Transitions**: Realistic storm growth, movement, and dissipation over time
# - **GeoJSON Format**: Standards-compliant GeoJSON output for map visualization
# - **Event Hub Integration**: Ready for real-time streaming to Azure Event Hub
# - **Configurable Timeline**: 3-minute progression + 1-minute dissipation with 10-second intervals
# 
# ### 🎯 Key Simulation Parameters:
# - **Total Duration**: 4 minutes (240 seconds)
# - **Frame Rate**: Every 10 seconds (24 total frames)
# - **Storm Movement**: West to east across City
# - **Intensity Zones**: Green (light), Yellow (moderate), Orange (heavy), Red (extra heavy)
# - **Realistic Physics**: Storm growth, peak intensity, and exponential dissipation
# 
# ### 🔧 Usage Instructions:
# 
# 1. **Configure Event Hub**: Update the `EVENT_HUB_CONFIG` with your Azure Event Hub connection string
# 2. **Run Simulation**: Execute the full simulation cell for real-time 4-minute experience
# 3. **Accelerated Testing**: Use accelerated mode for quick testing and validation
# 4. **Data Analysis**: Built-in analysis and visualization tools for storm data
# 
# ### 🌐 Event Hub Message Format:
# Each message contains a GeoJSON FeatureCollection with:
# - Storm system metadata (ID, timestamp, phase, center location)
# - Multiple polygon features for each intensity zone
# - Color codes and rainfall rates for visualization
# - Frame sequencing information for animation
# 
# ### 📊 Visualization Ready:
# The generated GeoJSON can be directly consumed by mapping libraries like:
# - Leaflet.js
# - Mapbox GL JS  
# - Azure Maps
# - Google Maps
# - ArcGIS APIs
# 
# ### 🚀 Next Steps:
# - Connect to real Azure Event Hub for live streaming
# - Integrate with real-time weather data sources
# - Add multiple storm systems or different weather patterns
# - Implement storm prediction and forecasting features
# - Create web-based visualization dashboard

