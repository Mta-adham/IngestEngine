"""
UK World Model - Comprehensive API & Data Clients
==================================================

40+ clients for building a complete UK World Model with:
- Building opening/construction dates
- Historical web data via Wayback Machine
- Street-level imagery dating
- Business directories (web scraping)
- POI data from multiple sources

=== CORE GOVERNMENT & BUSINESS ===
- CompaniesHouseClient: Company profiles, officers, filings
- CompaniesHouseStreamingClient: Real-time company changes
- LandRegistryClient: Property transactions, HPI
- DVLAClient: Vehicle registration data
- GovUKClient: GOV.UK content, registers, bank holidays
- VOAClient: Business rates, council tax valuations

=== TRANSPORT ===
- TfLClient: London transport (tube, bus, bikes)
- NetworkRailClient: National rail stations and data

=== HEALTH & SOCIAL ===
- NHSClient: Hospitals, GPs, pharmacies
- CQCClient: Care Quality Commission inspections
- FoodStandardsClient: Food hygiene ratings

=== PROPERTY & PLANNING ===
- EPCClient: Energy Performance Certificates (construction dates!)
- PlanningClient: Planning applications and constraints
- OSDataHubClient: Ordnance Survey addresses and maps
- VOAClient: Valuation Office data

=== EDUCATION & CHARITIES ===
- OfstedClient: School inspection data
- CharitiesClient: Charity Commission data

=== SAFETY & ENVIRONMENT ===
- PoliceUKClient: Crime data
- EnvironmentAgencyClient: Flood warnings, monitoring
- DEFRAClient: Air quality, protected sites
- MetOfficeClient: Weather forecasts
- BGSClient: British Geological Survey (ground data)
- LIDARClient: Elevation and building height data

=== HERITAGE & CULTURE ===
- HistoricEnglandClient: Listed buildings, monuments

=== POLITICS & GOVERNANCE ===
- UKParliamentClient: MPs, Lords, Bills
- ElectoralCommissionClient: Parties, donations

=== ECONOMY & STATISTICS ===
- ONSClient: Postcodes, demographics
- NOMISClient: Labour market statistics

=== TELECOMMUNICATIONS ===
- OfcomClient: Broadband and mobile coverage

=== GEOGRAPHIC & MAPPING ===
- OpenStreetMapClient: POIs, buildings, boundaries

=== HISTORICAL DATA ===
- WaybackMachineClient: Internet Archive historical snapshots
- UKWebArchiveClient: British Library UK web archive
- StreetViewClient: Street-level imagery dates

=== POI DATA ===
- GooglePlacesClient: Google Places API
- FoursquareClient: Foursquare Places API
- WebScraperClient: Yell, Rightmove, 192.com scraping

=== BUILDING DATES ===
- BuildingDateEstimator: Multi-source building age estimation

Usage:
    # Single client
    from src.clients import LandRegistryClient
    lr = LandRegistryClient()
    prices = lr.get_price_paid_df(postcode="SW1A")
    
    # Building date estimation
    from src.clients import BuildingDateEstimator
    estimator = BuildingDateEstimator()
    dates = estimator.estimate_building_date(postcode="SW1A 1AA")
    
    # Historical web data
    from src.clients import WaybackMachineClient
    wayback = WaybackMachineClient()
    first_date = wayback.get_first_appearance_date("https://tesco.com")
"""

from src.clients.base_client import BaseAPIClient

# === CORE GOVERNMENT & BUSINESS ===
from src.clients.companies_house import CompaniesHouseClient
from src.clients.companies_house_streaming import CompaniesHouseStreamingClient
from src.clients.land_registry import LandRegistryClient
from src.clients.dvla import DVLAClient
from src.clients.gov_uk import GovUKClient
from src.clients.voa import VOAClient

# === TRANSPORT ===
from src.clients.tfl import TfLClient
from src.clients.network_rail import NetworkRailClient

# === HEALTH & SOCIAL ===
from src.clients.nhs import NHSClient
from src.clients.cqc import CQCClient
from src.clients.food_standards import FoodStandardsClient

# === PROPERTY & PLANNING ===
from src.clients.epc import EPCClient
from src.clients.planning import PlanningClient
from src.clients.os_datahub import OSDataHubClient

# === EDUCATION & CHARITIES ===
from src.clients.ofsted import OfstedClient
from src.clients.charities import CharitiesClient

# === SAFETY & ENVIRONMENT ===
from src.clients.police_uk import PoliceUKClient
from src.clients.environment_agency import EnvironmentAgencyClient
from src.clients.defra import DEFRAClient
from src.clients.met_office import MetOfficeClient
from src.clients.bgs import BGSClient
from src.clients.lidar import LIDARClient

# === HERITAGE & CULTURE ===
from src.clients.historic_england import HistoricEnglandClient

# === POLITICS & GOVERNANCE ===
from src.clients.uk_parliament import UKParliamentClient
from src.clients.electoral_commission import ElectoralCommissionClient

# === ECONOMY & STATISTICS ===
from src.clients.ons import ONSClient
from src.clients.nomis import NOMISClient

# === TELECOMMUNICATIONS ===
from src.clients.ofcom import OfcomClient

# === GEOGRAPHIC & MAPPING ===
from src.clients.openstreetmap import OpenStreetMapClient

# === HISTORICAL DATA ===
from src.clients.wayback_machine import WaybackMachineClient
from src.clients.uk_web_archive import UKWebArchiveClient
from src.clients.streetview import StreetViewClient

# === POI DATA ===
from src.clients.google_places import GooglePlacesClient
from src.clients.foursquare import FoursquareClient
from src.clients.web_scraper import WebScraperClient

# === BUILDING DATES ===
from src.clients.building_dates import BuildingDateEstimator

# === UNIFIED ===
from src.clients.unified_fetcher import UnifiedDataFetcher

__all__ = [
    # Base
    'BaseAPIClient',
    
    # Core Government & Business
    'CompaniesHouseClient',
    'CompaniesHouseStreamingClient',
    'LandRegistryClient',
    'DVLAClient',
    'GovUKClient',
    'VOAClient',
    
    # Transport
    'TfLClient',
    'NetworkRailClient',
    
    # Health & Social
    'NHSClient',
    'CQCClient',
    'FoodStandardsClient',
    
    # Property & Planning
    'EPCClient',
    'PlanningClient',
    'OSDataHubClient',
    
    # Education & Charities
    'OfstedClient',
    'CharitiesClient',
    
    # Safety & Environment
    'PoliceUKClient',
    'EnvironmentAgencyClient',
    'DEFRAClient',
    'MetOfficeClient',
    'BGSClient',
    'LIDARClient',
    
    # Heritage & Culture
    'HistoricEnglandClient',
    
    # Politics & Governance
    'UKParliamentClient',
    'ElectoralCommissionClient',
    
    # Economy & Statistics
    'ONSClient',
    'NOMISClient',
    
    # Telecommunications
    'OfcomClient',
    
    # Geographic & Mapping
    'OpenStreetMapClient',
    
    # Historical Data
    'WaybackMachineClient',
    'UKWebArchiveClient',
    'StreetViewClient',
    
    # POI Data
    'GooglePlacesClient',
    'FoursquareClient',
    'WebScraperClient',
    
    # Building Dates
    'BuildingDateEstimator',
    
    # Unified
    'UnifiedDataFetcher',
]

# Convenience aliases
UnifiedWorldModelFetcher = UnifiedDataFetcher
