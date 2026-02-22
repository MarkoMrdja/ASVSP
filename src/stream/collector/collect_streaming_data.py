#!/usr/bin/env python3
"""
Streaming Data Collector for US Air Quality Analysis System

Collects real-time air quality and weather data from:
- Google Air Quality API  (hourly, requires GOOGLE_AIR_QUALITY_API_KEY)
- Open-Meteo Weather API  (hourly, free)

All data is stored in a single SQLite database: data/streaming/measurements.db

Tables:
  cities        — static reference: one row per city with selection rationale
  air_quality   — one row per (city, poll cycle)
  weather       — one row per (city, poll cycle)
"""

import os
import sqlite3
import time
import logging
import requests
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POLL_INTERVAL_MINUTES = 60
_HERE = Path(__file__).parent          # src/stream/collector/  (or /app/ in Docker)
DB_PATH = _HERE / "data" / "measurements.db"
LOG_PATH = _HERE / "logs" / "collector.log"
GOOGLE_API_KEY = os.environ.get("GOOGLE_AIR_QUALITY_API_KEY")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# City list — 80 cities across the US
#
# Each entry:
#   city, state, lat, lon, category, reason
#
# Categories:
#   dense_urban      — major population centres, heavy traffic emissions
#   industrial       — heavy industry, steel, coal, chemical, refining
#   wildfire_smoke   — Pacific/Mountain West cities with recurring smoke events
#   ozone_belt       — hot, sunny cities in the southwest ozone formation zone
#   coastal          — coastal/maritime air-mass dynamics
#   agricultural     — farm dust, pesticide drift, confined animal feeding
#   rural_baseline   — low-emission reference points for statistical contrast
#   energy           — oil & gas extraction / refinery corridors
# ---------------------------------------------------------------------------

LOCATIONS = [
    # ── Original 20 ────────────────────────────────────────────────────────
    {
        "city": "Los Angeles", "state": "California",
        "lat": 34.0522, "lon": -118.2437,
        "category": "dense_urban",
        "reason": (
            "Largest US city by area; notorious smog history driven by traffic, "
            "port diesel, and ozone chemistry trapped by the San Gabriel Mountains."
        ),
    },
    {
        "city": "San Francisco", "state": "California",
        "lat": 37.7749, "lon": -122.4194,
        "category": "wildfire_smoke",
        "reason": (
            "Bay Area experiences severe wildfire smoke intrusions (Camp Fire 2018 "
            "recorded worst AQI in world history for a major city); marine fog layer "
            "traps particulates."
        ),
    },
    {
        "city": "New York", "state": "New York",
        "lat": 40.7128, "lon": -74.0060,
        "category": "dense_urban",
        "reason": (
            "Most densely populated US city; dense traffic, building heating oil, "
            "and occasional regional ozone transport from the Ohio Valley."
        ),
    },
    {
        "city": "Chicago", "state": "Illinois",
        "lat": 41.8781, "lon": -87.6298,
        "category": "dense_urban",
        "reason": (
            "Third-largest US city; major truck freight hub (I-90/I-94 corridor), "
            "historic industrial Southeast Side, and Great Lakes wind patterns."
        ),
    },
    {
        "city": "Houston", "state": "Texas",
        "lat": 29.7604, "lon": -95.3698,
        "category": "industrial",
        "reason": (
            "Largest US petrochemical complex; Ship Channel refineries and chemical "
            "plants produce some of the highest industrial VOC and NOx emissions in "
            "the country."
        ),
    },
    {
        "city": "Phoenix", "state": "Arizona",
        "lat": 33.4484, "lon": -112.0740,
        "category": "ozone_belt",
        "reason": (
            "Extreme heat amplifies ozone chemistry; dust storms (haboobs) cause "
            "severe PM10 spikes; rapid suburban sprawl increases vehicle miles."
        ),
    },
    {
        "city": "Philadelphia", "state": "Pennsylvania",
        "lat": 39.9526, "lon": -75.1652,
        "category": "dense_urban",
        "reason": (
            "Mid-Atlantic corridor city; downwind of Ohio Valley industrial emissions; "
            "historical refinery presence in South Philly."
        ),
    },
    {
        "city": "San Antonio", "state": "Texas",
        "lat": 29.4241, "lon": -98.4936,
        "category": "ozone_belt",
        "reason": (
            "Recurring ozone non-attainment driven by regional oil & gas activity "
            "and hot summers; provides Texas contrast to Houston's industrial profile."
        ),
    },
    {
        "city": "San Diego", "state": "California",
        "lat": 32.7157, "lon": -117.1611,
        "category": "coastal",
        "reason": (
            "Coastal marine layer interacts with cross-border pollution from Tijuana "
            "and regional wildfires; military jet activity adds localized NOx."
        ),
    },
    {
        "city": "Dallas", "state": "Texas",
        "lat": 32.7767, "lon": -96.7970,
        "category": "ozone_belt",
        "reason": (
            "DFW metroplex is a persistent ozone non-attainment area; hot summers, "
            "high vehicle miles, and Barnett Shale gas emissions."
        ),
    },
    {
        "city": "Denver", "state": "Colorado",
        "lat": 39.7392, "lon": -104.9903,
        "category": "ozone_belt",
        "reason": (
            "High-altitude city in Front Range ozone non-attainment zone; oil & gas "
            "VOC emissions from Weld County combine with urban traffic under "
            "temperature inversions."
        ),
    },
    {
        "city": "Seattle", "state": "Washington",
        "lat": 47.6062, "lon": -122.3321,
        "category": "wildfire_smoke",
        "reason": (
            "Heavy wildfire smoke from eastern Washington, Oregon, and British "
            "Columbia; Puget Sound geography channels smoke accumulation."
        ),
    },
    {
        "city": "Boston", "state": "Massachusetts",
        "lat": 42.3601, "lon": -71.0589,
        "category": "dense_urban",
        "reason": (
            "Northeast corridor urban core; winter wood-burning and traffic; "
            "receives transported ozone from upwind Mid-Atlantic states."
        ),
    },
    {
        "city": "Atlanta", "state": "Georgia",
        "lat": 33.7490, "lon": -84.3880,
        "category": "dense_urban",
        "reason": (
            "Major Southeast hub; high car dependency, tree canopy biogenic VOC "
            "emissions combine with traffic NOx for intense ozone formation."
        ),
    },
    {
        "city": "Miami", "state": "Florida",
        "lat": 25.7617, "lon": -80.1918,
        "category": "coastal",
        "reason": (
            "Tropical coastal city; sea-salt PM, vehicle emissions, and occasional "
            "African dust plumes; provides warm-climate baseline."
        ),
    },
    {
        "city": "Detroit", "state": "Michigan",
        "lat": 42.3314, "lon": -83.0458,
        "category": "industrial",
        "reason": (
            "Auto manufacturing capital; Southeast Michigan has historically had "
            "among the highest industrial PM2.5 and SO2 levels in the Midwest."
        ),
    },
    {
        "city": "Minneapolis", "state": "Minnesota",
        "lat": 44.9778, "lon": -93.2650,
        "category": "dense_urban",
        "reason": (
            "Northern metro; cold-weather wood-burning and vehicle cold-start "
            "emissions; receives smoke from Canadian boreal fires."
        ),
    },
    {
        "city": "Las Vegas", "state": "Nevada",
        "lat": 36.1699, "lon": -115.1398,
        "category": "ozone_belt",
        "reason": (
            "Desert valley heat drives ozone; construction dust and casino strip "
            "diesel generators contribute PM; growth corridor for comparison."
        ),
    },
    {
        "city": "Portland", "state": "Oregon",
        "lat": 45.5152, "lon": -122.6784,
        "category": "wildfire_smoke",
        "reason": (
            "Willamette Valley acts as a funnel for smoke from Oregon and California "
            "fires; winter wood-burning adds winter PM2.5 episodes."
        ),
    },
    {
        "city": "Salt Lake City", "state": "Utah",
        "lat": 40.7608, "lon": -111.8910,
        "category": "industrial",
        "reason": (
            "Trapped in a valley with severe winter temperature inversions; Kennecott "
            "copper smelter (one of the largest open-pit mines in the world) and "
            "I-15 corridor produce worst winter PM2.5 in the Mountain West."
        ),
    },

    # ── Industrial corridor ─────────────────────────────────────────────────
    {
        "city": "Pittsburgh", "state": "Pennsylvania",
        "lat": 40.4406, "lon": -79.9959,
        "category": "industrial",
        "reason": (
            "Former steel capital; river valley topography traps pollution; still "
            "has active coke plants and a legacy of industrial PM2.5 and SO2."
        ),
    },
    {
        "city": "Birmingham", "state": "Alabama",
        "lat": 33.5186, "lon": -86.8104,
        "category": "industrial",
        "reason": (
            "Iron and steel manufacturing heritage; Jones Valley geography creates "
            "the same inversion-trap dynamic as Pittsburgh; historically top-10 "
            "worst particle pollution cities."
        ),
    },
    {
        "city": "Cleveland", "state": "Ohio",
        "lat": 41.4993, "lon": -81.6944,
        "category": "industrial",
        "reason": (
            "Cuyahoga Valley industrial corridor; steel, auto parts, and chemical "
            "manufacturing; Lake Erie shore effects on PM dispersion."
        ),
    },
    {
        "city": "Cincinnati", "state": "Ohio",
        "lat": 39.1031, "lon": -84.5120,
        "category": "industrial",
        "reason": (
            "Ohio River valley traps Ohio/Kentucky industrial SO2 and NOx; "
            "historically one of the worst ozone non-attainment metros in the East."
        ),
    },
    {
        "city": "Louisville", "state": "Kentucky",
        "lat": 38.2527, "lon": -85.7585,
        "category": "industrial",
        "reason": (
            "Downwind of Indiana coal plants; Ohio River valley inversion events; "
            "home to major chemical and pharmaceutical manufacturing."
        ),
    },
    {
        "city": "St. Louis", "state": "Missouri",
        "lat": 38.6270, "lon": -90.1994,
        "category": "industrial",
        "reason": (
            "Mississippi-Missouri confluence industrial hub; historically one of the "
            "highest SO2 cities due to coal power plants upwind in Illinois."
        ),
    },
    {
        "city": "Gary", "state": "Indiana",
        "lat": 41.5934, "lon": -87.3464,
        "category": "industrial",
        "reason": (
            "Active US Steel works (largest integrated steel plant in North America); "
            "consistently records among the highest PM2.5 and SO2 in the Midwest."
        ),
    },
    {
        "city": "Baton Rouge", "state": "Louisiana",
        "lat": 30.4515, "lon": -91.1871,
        "category": "industrial",
        "reason": (
            "Part of the 'Cancer Alley' petrochemical corridor; ExxonMobil's largest "
            "US refinery sits here; high benzene, VOC, and SO2 readings."
        ),
    },

    # ── Wildfire smoke ──────────────────────────────────────────────────────
    {
        "city": "Sacramento", "state": "California",
        "lat": 38.5816, "lon": -121.4944,
        "category": "wildfire_smoke",
        "reason": (
            "Central Valley geography traps Sierra Nevada wildfire smoke; surrounded "
            "by high-fire-risk foothill communities; PM2.5 regularly exceeds 150 "
            "µg/m³ during fire season."
        ),
    },
    {
        "city": "Fresno", "state": "California",
        "lat": 36.7378, "lon": -119.7871,
        "category": "wildfire_smoke",
        "reason": (
            "San Joaquin Valley has the worst particulate pollution in the US; "
            "surrounded by agricultural dust, diesel freight (Hwy 99), and wildfire "
            "smoke with no exit route due to valley walls."
        ),
    },
    {
        "city": "Bakersfield", "state": "California",
        "lat": 35.3733, "lon": -119.0187,
        "category": "wildfire_smoke",
        "reason": (
            "Consistently ranked #1 worst air quality in the US (American Lung "
            "Association annual reports); Kern County oil fields add VOC; valley "
            "topography concentrates all pollutants."
        ),
    },
    {
        "city": "Boise", "state": "Idaho",
        "lat": 43.6150, "lon": -116.2023,
        "category": "wildfire_smoke",
        "reason": (
            "Treasure Valley receives smoke from Idaho, Oregon, and Montana fires; "
            "one of the fastest-growing US metros, increasing baseline emissions."
        ),
    },
    {
        "city": "Spokane", "state": "Washington",
        "lat": 47.6588, "lon": -117.4260,
        "category": "wildfire_smoke",
        "reason": (
            "Eastern Washington smoke corridor; wood-burning in winter and wildfire "
            "smoke in summer create year-round PM2.5 issues; EPA non-attainment area."
        ),
    },
    {
        "city": "Missoula", "state": "Montana",
        "lat": 46.8721, "lon": -113.9940,
        "category": "wildfire_smoke",
        "reason": (
            "University town in a bowl-shaped valley; smoke from Montana/Idaho fires "
            "pools here; used in academic smoke-health studies as a reference site."
        ),
    },

    # ── Ozone belt / Southwest ──────────────────────────────────────────────
    {
        "city": "Albuquerque", "state": "New Mexico",
        "lat": 35.0844, "lon": -106.6504,
        "category": "ozone_belt",
        "reason": (
            "High-elevation desert city; intense UV accelerates ozone formation; "
            "Permian Basin oil & gas precursor transport from southeastern NM."
        ),
    },
    {
        "city": "El Paso", "state": "Texas",
        "lat": 31.7619, "lon": -106.4850,
        "category": "ozone_belt",
        "reason": (
            "Binational airshed shared with Ciudad Juárez; cross-border industrial "
            "and vehicle emissions complicate attainment; desert dust adds PM10."
        ),
    },
    {
        "city": "Tucson", "state": "Arizona",
        "lat": 32.2226, "lon": -110.9747,
        "category": "ozone_belt",
        "reason": (
            "Sonoran Desert heat drives ozone; copper smelting at nearby Hayden "
            "adds SO2; seasonal dust storms from bare desert surfaces."
        ),
    },
    {
        "city": "Oklahoma City", "state": "Oklahoma",
        "lat": 35.4676, "lon": -97.5164,
        "category": "energy",
        "reason": (
            "Oil & gas production at the heart of the SCOOP/STACK plays; methane "
            "and VOC emissions from thousands of wells; tornadic weather mixes "
            "pollutants unpredictably."
        ),
    },

    # ── Energy / Oil & Gas ──────────────────────────────────────────────────
    {
        "city": "Midland", "state": "Texas",
        "lat": 31.9973, "lon": -102.0779,
        "category": "energy",
        "reason": (
            "Ground zero for Permian Basin shale boom; flaring, compressor stations, "
            "and tank batteries emit enormous methane and VOC loads; one of the "
            "fastest-growing ozone problem areas in the US."
        ),
    },
    {
        "city": "Casper", "state": "Wyoming",
        "lat": 42.8666, "lon": -106.3131,
        "category": "energy",
        "reason": (
            "Wyoming is the top coal-producing state; Casper sits amid oil & gas "
            "fields; provides energy-sector baseline for Mountain West comparisons."
        ),
    },
    {
        "city": "Anchorage", "state": "Alaska",
        "lat": 61.2181, "lon": -149.9003,
        "category": "rural_baseline",
        "reason": (
            "Geographically isolated; provides high-latitude Arctic-influenced "
            "baseline; winter temperature inversions create localized PM events "
            "unusual for a low-density city."
        ),
    },

    # ── Coastal / Maritime ──────────────────────────────────────────────────
    {
        "city": "Baltimore", "state": "Maryland",
        "lat": 39.2904, "lon": -76.6122,
        "category": "coastal",
        "reason": (
            "Major port city; heavy truck and ship diesel at the Port of Baltimore; "
            "Chesapeake Bay sea-breeze circulation traps ozone from DC metro."
        ),
    },
    {
        "city": "New Orleans", "state": "Louisiana",
        "lat": 29.9511, "lon": -90.0715,
        "category": "coastal",
        "reason": (
            "Mississippi River delta industrial corridor; offshore oil & gas support "
            "base; Gulf Coast sea-breeze dynamics and hurricane-season disruptions."
        ),
    },
    {
        "city": "Tampa", "state": "Florida",
        "lat": 27.9506, "lon": -82.4572,
        "category": "coastal",
        "reason": (
            "Gulf Coast phosphate industry (largest in the US) generates fluoride "
            "and PM emissions; sea-breeze convergence zones focus afternoon storms "
            "and ozone formation."
        ),
    },
    {
        "city": "Virginia Beach", "state": "Virginia",
        "lat": 36.8529, "lon": -75.9780,
        "category": "coastal",
        "reason": (
            "Mid-Atlantic coastal baseline; military jet activity (NAS Oceana) adds "
            "localized NOx; represents cleaner coastal air for contrast."
        ),
    },

    # ── Dense urban — East Coast ────────────────────────────────────────────
    {
        "city": "Washington", "state": "District of Columbia",
        "lat": 38.9072, "lon": -77.0369,
        "category": "dense_urban",
        "reason": (
            "High traffic density from commuters; downwind of Ohio Valley SO2; "
            "Chesapeake Bay ozone transport; politically relevant monitoring location."
        ),
    },
    {
        "city": "Newark", "state": "New Jersey",
        "lat": 40.7357, "lon": -74.1724,
        "category": "industrial",
        "reason": (
            "Port Newark/Elizabeth Complex is the busiest container port on the East "
            "Coast; truck diesel, ship exhaust, and petroleum tank farms make this "
            "one of the most polluted zip codes in the US."
        ),
    },
    {
        "city": "Charlotte", "state": "North Carolina",
        "lat": 35.2271, "lon": -80.8431,
        "category": "dense_urban",
        "reason": (
            "Fastest-growing large city in the Southeast; sprawling highway network "
            "with high vehicle miles; biogenic VOC from surrounding forests."
        ),
    },
    {
        "city": "Nashville", "state": "Tennessee",
        "lat": 36.1627, "lon": -86.7816,
        "category": "dense_urban",
        "reason": (
            "Rapidly growing metro; TVA coal plant legacy; terrain channels pollution "
            "from surrounding counties into the basin."
        ),
    },
    {
        "city": "Richmond", "state": "Virginia",
        "lat": 37.5407, "lon": -77.4360,
        "category": "dense_urban",
        "reason": (
            "James River valley traps transported ozone from DC/Philly corridor; "
            "Dominion Energy power plants historically contributed SO2."
        ),
    },
    {
        "city": "Memphis", "state": "Tennessee",
        "lat": 35.1495, "lon": -90.0490,
        "category": "industrial",
        "reason": (
            "Major freight rail and trucking hub (FedEx world hub); Mississippi River "
            "barge diesel; surrounded by Arkansas industrial facilities."
        ),
    },

    # ── Dense urban — Midwest / South ──────────────────────────────────────
    {
        "city": "Kansas City", "state": "Missouri",
        "lat": 39.0997, "lon": -94.5786,
        "category": "dense_urban",
        "reason": (
            "Major rail and truck freight nexus; livestock processing odors and VOC; "
            "interstate I-70/I-35 diesel corridor."
        ),
    },
    {
        "city": "Indianapolis", "state": "Indiana",
        "lat": 39.7684, "lon": -86.1581,
        "category": "industrial",
        "reason": (
            "Downwind of multiple Indiana coal plants; pharmaceutical manufacturing "
            "VOC emissions; flat terrain offers minimal natural dispersion."
        ),
    },
    {
        "city": "Columbus", "state": "Ohio",
        "lat": 39.9612, "lon": -82.9988,
        "category": "dense_urban",
        "reason": (
            "Largest Ohio city; surrounded by Ohio coal plant SO2/NOx transport; "
            "growing data-center and logistics corridor adds diesel load."
        ),
    },
    {
        "city": "Milwaukee", "state": "Wisconsin",
        "lat": 43.0389, "lon": -87.9065,
        "category": "industrial",
        "reason": (
            "Lake Michigan shore effects concentrate pollution; industrial south side "
            "has heavy metals and VOC from manufacturing; receives Chicago plume "
            "on southerly winds."
        ),
    },
    {
        "city": "Omaha", "state": "Nebraska",
        "lat": 41.2565, "lon": -95.9345,
        "category": "agricultural",
        "reason": (
            "Nebraska Corn Belt gateway; livestock feedlots and meat-packing plants "
            "generate ammonia and hydrogen sulfide; agriculture-urban interface city."
        ),
    },

    # ── Agricultural ───────────────────────────────────────────────────────
    {
        "city": "Stockton", "state": "California",
        "lat": 37.9577, "lon": -121.2908,
        "category": "agricultural",
        "reason": (
            "San Joaquin Valley agriculture — pesticide spray drift, crop residue "
            "burning, and harvest dust; also a major inland port with diesel freight."
        ),
    },
    {
        "city": "Visalia", "state": "California",
        "lat": 36.3302, "lon": -119.2921,
        "category": "agricultural",
        "reason": (
            "Heart of the San Joaquin Valley dairy/citrus belt; dairy cow emissions "
            "(ammonia, methane) react with NOx to form secondary PM2.5; one of the "
            "worst particle pollution counties in the US."
        ),
    },
    {
        "city": "Lubbock", "state": "Texas",
        "lat": 33.5779, "lon": -101.8552,
        "category": "agricultural",
        "reason": (
            "Texas Panhandle cotton and grain farming; severe dust storms from bare "
            "tilled fields and Saharan-style wind erosion; provides agricultural "
            "PM10 baseline."
        ),
    },
    {
        "city": "Des Moines", "state": "Iowa",
        "lat": 41.5868, "lon": -93.6250,
        "category": "agricultural",
        "reason": (
            "Iowa corn/soybean belt capital; concentrated animal feeding operations "
            "nearby; ethanol plant VOC emissions; spring fertilizer application "
            "creates ammonia spikes."
        ),
    },

    # ── Rural baseline ──────────────────────────────────────────────────────
    {
        "city": "Billings", "state": "Montana",
        "lat": 45.7833, "lon": -108.5007,
        "category": "rural_baseline",
        "reason": (
            "Largest Montana city but still low-density; ExxonMobil refinery is the "
            "one significant industrial source, making it ideal to isolate refinery "
            "impact on an otherwise clean background."
        ),
    },
    {
        "city": "Cheyenne", "state": "Wyoming",
        "lat": 41.1400, "lon": -104.8202,
        "category": "rural_baseline",
        "reason": (
            "High-plains clean-air baseline; minimal local industry; useful reference "
            "for distinguishing regional transport from local sources."
        ),
    },
    {
        "city": "Fargo", "state": "North Dakota",
        "lat": 46.8772, "lon": -96.7898,
        "category": "rural_baseline",
        "reason": (
            "Northern Great Plains baseline; Red River Valley crop burning events in "
            "spring; monitors Canadian air mass inflow."
        ),
    },
    {
        "city": "Burlington", "state": "Vermont",
        "lat": 44.4759, "lon": -73.2121,
        "category": "rural_baseline",
        "reason": (
            "Cleanest large metro in the Northeast; Lake Champlain airshed; useful "
            "reference point for comparing against New York / Boston transport."
        ),
    },
    {
        "city": "Honolulu", "state": "Hawaii",
        "lat": 21.3069, "lon": -157.8583,
        "category": "rural_baseline",
        "reason": (
            "Remote island baseline; primary local source is Kilauea volcanic SO2 "
            "(vog); otherwise represents the cleanest achievable US urban air."
        ),
    },

    # ── Additional dense urban ──────────────────────────────────────────────
    {
        "city": "Austin", "state": "Texas",
        "lat": 30.2672, "lon": -97.7431,
        "category": "dense_urban",
        "reason": (
            "Fastest-growing major US city; rapidly rising vehicle miles; Cedar "
            "pollen aerosol events in winter; downwind of Permian Basin VOC transport."
        ),
    },
    {
        "city": "Jacksonville", "state": "Florida",
        "lat": 30.3322, "lon": -81.6557,
        "category": "dense_urban",
        "reason": (
            "Largest US city by land area; paper mill H2S and SO2; port diesel; "
            "warm humid climate accelerates secondary PM formation."
        ),
    },
    {
        "city": "Sacramento", "state": "California",
        "lat": 38.5816, "lon": -121.4944,
        "category": "wildfire_smoke",
        "reason": (
            "Central Valley geography traps Sierra Nevada wildfire smoke; surrounded "
            "by high-fire-risk foothill communities; PM2.5 regularly exceeds 150 "
            "µg/m³ during fire season."
        ),
    },
    {
        "city": "Oakland", "state": "California",
        "lat": 37.8044, "lon": -122.2712,
        "category": "industrial",
        "reason": (
            "Port of Oakland is the fifth-busiest container port in the US; diesel "
            "truck and ship emissions heavily impact West Oakland communities; "
            "historically among the highest childhood asthma rates in California."
        ),
    },
    {
        "city": "Long Beach", "state": "California",
        "lat": 33.7701, "lon": -118.1937,
        "category": "industrial",
        "reason": (
            "Ports of LA/Long Beach handle 40% of US container imports; diesel "
            "ship, truck, and rail emissions create a chronic pollution hotspot; "
            "adjacent to LA ozone basin."
        ),
    },
    {
        "city": "Riverside", "state": "California",
        "lat": 33.9806, "lon": -117.3755,
        "category": "ozone_belt",
        "reason": (
            "Inland Empire sits at the downwind end of the LA basin ozone conveyor; "
            "historically records highest ozone levels in the US; major warehouse "
            "and logistics diesel corridor."
        ),
    },
    {
        "city": "Reno", "state": "Nevada",
        "lat": 39.5296, "lon": -119.8138,
        "category": "wildfire_smoke",
        "reason": (
            "Truckee Meadows valley traps smoke from Sierra Nevada and Nevada fires; "
            "rapidly growing warehouse/data-center corridor adds diesel load."
        ),
    },
    {
        "city": "Tulsa", "state": "Oklahoma",
        "lat": 36.1540, "lon": -95.9928,
        "category": "energy",
        "reason": (
            "Historic oil capital of the world; ONEOK natural gas processing; "
            "Arkansas River industrial corridor; compares with OKC on same state "
            "air-mass to isolate urban vs. energy source effects."
        ),
    },
    {
        "city": "Knoxville", "state": "Tennessee",
        "lat": 35.9606, "lon": -83.9207,
        "category": "industrial",
        "reason": (
            "Great Smoky Mountains haze hotspot; TVA Kingston coal ash spill legacy; "
            "valley topography traps regional transport from Ohio Valley."
        ),
    },
    {
        "city": "Little Rock", "state": "Arkansas",
        "lat": 34.7465, "lon": -92.2896,
        "category": "agricultural",
        "reason": (
            "Arkansas River Valley agriculture; rice field methane; timber and paper "
            "mill VOC; monitors the rural South-Central air quality gap."
        ),
    },
    {
        "city": "Columbia", "state": "South Carolina",
        "lat": 34.0007, "lon": -81.0348,
        "category": "dense_urban",
        "reason": (
            "South Carolina's largest metro; downwind of Charlotte industrial "
            "corridor; provides Southeast inland contrast to coastal cities."
        ),
    },
    {
        "city": "Sioux Falls", "state": "South Dakota",
        "lat": 43.5446, "lon": -96.7311,
        "category": "agricultural",
        "reason": (
            "Northern Plains agricultural hub; meatpacking plants (Tyson, Smithfield) "
            "generate hydrogen sulfide and ammonia; represents Great Plains "
            "agricultural air quality."
        ),
    },
    {
        "city": "Lexington", "state": "Kentucky",
        "lat": 38.0406, "lon": -84.5037,
        "category": "industrial",
        "reason": (
            "Kentucky Bluegrass region; downwind of multiple Ohio River coal plants; "
            "horse farm dust adds biological PM; tobacco curing historically "
            "contributed VOC."
        ),
    },
    {
        "city": "Providence", "state": "Rhode Island",
        "lat": 41.8240, "lon": -71.4128,
        "category": "dense_urban",
        "reason": (
            "Southern New England ozone corridor; receives transported ozone from "
            "New York on southwest flow; small industrial base adds local SO2."
        ),
    },
    {
        "city": "Hartford", "state": "Connecticut",
        "lat": 41.7658, "lon": -72.6851,
        "category": "dense_urban",
        "reason": (
            "Connecticut River Valley funnels New York metro ozone plume northward; "
            "historically among the highest ozone exceedance days in the Northeast."
        ),
    },
    {
        "city": "Allentown", "state": "Pennsylvania",
        "lat": 40.6084, "lon": -75.4902,
        "category": "industrial",
        "reason": (
            "Lehigh Valley cement and steel manufacturing legacy; downwind of the "
            "Reading/Lancaster industrial corridor; receives Philadelphia ozone "
            "transport on easterly sea-breeze."
        ),
    },
]

# De-duplicate by (city, state) — Sacramento was listed twice above
_seen = set()
_deduped = []
for _loc in LOCATIONS:
    _key = (_loc["city"], _loc["state"])
    if _key not in _seen:
        _seen.add(_key)
        _deduped.append(_loc)
LOCATIONS = _deduped


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create tables if they don't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS cities (
                id          INTEGER PRIMARY KEY,
                city        TEXT    NOT NULL,
                state       TEXT    NOT NULL,
                latitude    REAL    NOT NULL,
                longitude   REAL    NOT NULL,
                category    TEXT    NOT NULL,
                reason      TEXT    NOT NULL,
                UNIQUE(city, state)
            );

            CREATE TABLE IF NOT EXISTS air_quality (
                id                  INTEGER PRIMARY KEY,
                collected_at        TEXT    NOT NULL,
                city                TEXT    NOT NULL,
                state               TEXT    NOT NULL,
                aqi                 INTEGER,
                aqi_category        TEXT,
                dominant_pollutant  TEXT,
                pm25                REAL,
                pm10                REAL,
                o3                  REAL,
                no2                 REAL,
                so2                 REAL,
                co                  REAL
            );

            CREATE TABLE IF NOT EXISTS weather (
                id                  INTEGER PRIMARY KEY,
                collected_at        TEXT    NOT NULL,
                city                TEXT    NOT NULL,
                state               TEXT    NOT NULL,
                temperature_c       REAL,
                humidity_pct        INTEGER,
                wind_speed_kmh      REAL,
                wind_direction_deg  INTEGER,
                pressure_hpa        REAL,
                cloud_cover_pct     INTEGER,
                precipitation_mm    REAL
            );

            CREATE INDEX IF NOT EXISTS idx_aq_city_time
                ON air_quality(city, state, collected_at);
            CREATE INDEX IF NOT EXISTS idx_wx_city_time
                ON weather(city, state, collected_at);
        """)
    logger.info(f"Database initialised at {DB_PATH}")


def upsert_cities():
    """Populate the cities reference table from the LOCATIONS list."""
    with get_db() as conn:
        conn.executemany(
            """
            INSERT INTO cities (city, state, latitude, longitude, category, reason)
            VALUES (:city, :state, :lat, :lon, :category, :reason)
            ON CONFLICT(city, state) DO UPDATE SET
                latitude  = excluded.latitude,
                longitude = excluded.longitude,
                category  = excluded.category,
                reason    = excluded.reason
            """,
            LOCATIONS,
        )
    logger.info(f"Cities table populated with {len(LOCATIONS)} entries")


# ---------------------------------------------------------------------------
# API fetch functions
# ---------------------------------------------------------------------------

def get_current_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_air_quality(location: Dict) -> Optional[Dict]:
    if not GOOGLE_API_KEY:
        return None

    url = (
        "https://airquality.googleapis.com/v1/currentConditions:lookup"
        f"?key={GOOGLE_API_KEY}"
    )
    payload = {
        "location": {"latitude": location["lat"], "longitude": location["lon"]},
        "extraComputations": ["POLLUTANT_CONCENTRATION", "LOCAL_AQI"],
        "languageCode": "en",
    }

    try:
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Prefer usa_epa index, fall back to uaqi
        aqi = aqi_category = dominant_pollutant = None
        if "indexes" in data:
            by_code = {idx.get("code"): idx for idx in data["indexes"]}
            chosen = by_code.get("usa_epa") or by_code.get("uaqi")
            if chosen:
                aqi = chosen.get("aqi")
                aqi_category = chosen.get("category")
                dominant_pollutant = chosen.get("dominantPollutant")

        # Flatten pollutant concentrations
        conc: Dict[str, Optional[float]] = {p: None for p in ("pm25", "pm10", "o3", "no2", "so2", "co")}
        for p in data.get("pollutants", []):
            code = p.get("code", "").lower()
            if code in conc:
                conc[code] = p.get("concentration", {}).get("value")

        return {
            "collected_at": get_current_timestamp(),
            "city": location["city"],
            "state": location["state"],
            "aqi": aqi,
            "aqi_category": aqi_category,
            "dominant_pollutant": dominant_pollutant,
            **conc,
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Air quality fetch failed for {location['city']}: {e}")
        return None


def fetch_weather(location: Dict) -> Optional[Dict]:
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "current": (
            "temperature_2m,relative_humidity_2m,wind_speed_10m,"
            "wind_direction_10m,pressure_msl,cloud_cover,precipitation"
        ),
        "timezone": "auto",
    }

    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast", params=params, timeout=30
        )
        resp.raise_for_status()
        cur = resp.json().get("current", {})

        return {
            "collected_at": get_current_timestamp(),
            "city": location["city"],
            "state": location["state"],
            "temperature_c": cur.get("temperature_2m"),
            "humidity_pct": cur.get("relative_humidity_2m"),
            "wind_speed_kmh": cur.get("wind_speed_10m"),
            "wind_direction_deg": cur.get("wind_direction_10m"),
            "pressure_hpa": cur.get("pressure_msl"),
            "cloud_cover_pct": cur.get("cloud_cover"),
            "precipitation_mm": cur.get("precipitation"),
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Weather fetch failed for {location['city']}: {e}")
        return None


# ---------------------------------------------------------------------------
# Database write functions
# ---------------------------------------------------------------------------

def insert_air_quality(records: List[Dict]):
    with get_db() as conn:
        conn.executemany(
            """
            INSERT INTO air_quality
                (collected_at, city, state, aqi, aqi_category, dominant_pollutant,
                 pm25, pm10, o3, no2, so2, co)
            VALUES
                (:collected_at, :city, :state, :aqi, :aqi_category, :dominant_pollutant,
                 :pm25, :pm10, :o3, :no2, :so2, :co)
            """,
            records,
        )
    logger.info(f"Inserted {len(records)} air quality rows")


def insert_weather(records: List[Dict]):
    with get_db() as conn:
        conn.executemany(
            """
            INSERT INTO weather
                (collected_at, city, state, temperature_c, humidity_pct,
                 wind_speed_kmh, wind_direction_deg, pressure_hpa,
                 cloud_cover_pct, precipitation_mm)
            VALUES
                (:collected_at, :city, :state, :temperature_c, :humidity_pct,
                 :wind_speed_kmh, :wind_direction_deg, :pressure_hpa,
                 :cloud_cover_pct, :precipitation_mm)
            """,
            records,
        )
    logger.info(f"Inserted {len(records)} weather rows")


# ---------------------------------------------------------------------------
# Collection loop
# ---------------------------------------------------------------------------

def collect_all_data():
    aq_records: List[Dict] = []
    wx_records: List[Dict] = []

    for loc in LOCATIONS:
        logger.info(f"Collecting {loc['city']}, {loc['state']} ...")

        aq = fetch_air_quality(loc)
        if aq:
            aq_records.append(aq)

        wx = fetch_weather(loc)
        if wx:
            wx_records.append(wx)

        time.sleep(1)  # courtesy pause between cities

    if aq_records:
        insert_air_quality(aq_records)
    if wx_records:
        insert_weather(wx_records)

    logger.info(
        f"Cycle complete — {len(aq_records)} air quality, {len(wx_records)} weather rows written"
    )


def main():
    logger.info("=" * 60)
    logger.info("Streaming data collector starting")
    logger.info(f"Cities: {len(LOCATIONS)}")
    logger.info(f"Poll interval: {POLL_INTERVAL_MINUTES} minutes")
    logger.info(f"Database: {DB_PATH.resolve()}")
    logger.info("=" * 60)

    if not GOOGLE_API_KEY:
        logger.warning(
            "GOOGLE_AIR_QUALITY_API_KEY not set — "
            "air quality collection disabled; weather only"
        )

    init_db()
    upsert_cities()

    while True:
        try:
            collect_all_data()
            logger.info(f"Sleeping {POLL_INTERVAL_MINUTES} min until next cycle ...")
            time.sleep(POLL_INTERVAL_MINUTES * 60)

        except KeyboardInterrupt:
            logger.info("Stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    main()
