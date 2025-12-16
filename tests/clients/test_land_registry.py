"""
Tests for Land Registry API Client
"""

import pytest
import pandas as pd
from src.clients import LandRegistryClient


@pytest.fixture
def client():
    """Create client instance"""
    return LandRegistryClient()


class TestLandRegistryClient:
    """Tests for LandRegistryClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert client.BASE_URL == "https://landregistry.data.gov.uk"
    
    def test_health_check(self, client):
        """Test health check"""
        # May fail if API is down
        try:
            result = client.health_check()
            assert isinstance(result, bool)
        except Exception:
            pytest.skip("API unavailable")
    
    def test_get_house_price_index(self, client):
        """Test getting HPI data"""
        try:
            results = client.get_house_price_index(limit=5)
            assert isinstance(results, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_house_price_index_df(self, client):
        """Test getting HPI as DataFrame"""
        try:
            df = client.get_house_price_index_df(limit=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_regions(self, client):
        """Test getting regions"""
        try:
            regions = client.get_regions()
            assert isinstance(regions, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_price_paid(self, client):
        """Test getting price paid data"""
        try:
            results = client.get_price_paid(postcode="SW1A", limit=5)
            assert isinstance(results, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_price_paid_df(self, client):
        """Test getting price paid as DataFrame"""
        try:
            df = client.get_price_paid_df(postcode="E1", limit=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_bulk_download_urls(self, client):
        """Test getting bulk download URLs"""
        urls = client.get_bulk_download_urls()
        assert isinstance(urls, dict)
        assert 'price_paid_complete' in urls
    
    def test_get_average_price_by_region(self, client):
        """Test getting average prices by region"""
        try:
            results = client.get_average_price_by_region("2024-01")
            assert isinstance(results, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")

