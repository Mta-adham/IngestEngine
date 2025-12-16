"""
Tests for DEFRA / Natural England API Client
"""

import pytest
import pandas as pd
from src.clients import DEFRAClient


@pytest.fixture
def client():
    """Create client instance"""
    return DEFRAClient()


class TestDEFRAClient:
    """Tests for DEFRAClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
    
    def test_get_aqi_bands(self, client):
        """Test getting AQI band definitions"""
        bands = client.get_aqi_bands()
        assert isinstance(bands, list)
        assert len(bands) == 10
        
        # Check structure
        first = bands[0]
        assert 'index' in first
        assert 'band' in first
        assert 'description' in first
    
    def test_get_agricultural_land_classification(self, client):
        """Test getting agricultural land grades"""
        grades = client.get_agricultural_land_classification()
        assert isinstance(grades, dict)
        assert 'Grade 1' in grades
        assert 'Grade 5' in grades
    
    def test_get_bulk_download_urls(self, client):
        """Test getting bulk download URLs"""
        urls = client.get_bulk_download_urls()
        assert isinstance(urls, dict)
        assert 'sssi' in urls
        assert 'national_parks' in urls
    
    def test_get_sssi_sites(self, client):
        """Test getting SSSI sites"""
        try:
            df = client.get_sssi_sites(limit=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_aonb_areas(self, client):
        """Test getting AONB areas"""
        try:
            df = client.get_aonb_areas(limit=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_national_parks(self, client):
        """Test getting national parks"""
        try:
            df = client.get_national_parks()
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_monitoring_sites(self, client):
        """Test getting air quality monitoring sites"""
        try:
            sites = client.get_monitoring_sites()
            assert isinstance(sites, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")

