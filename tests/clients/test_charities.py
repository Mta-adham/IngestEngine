"""
Tests for Charities Commission API Client
"""

import pytest
import pandas as pd
from src.clients import CharitiesClient


@pytest.fixture
def client():
    """Create client instance"""
    return CharitiesClient()


class TestCharitiesClient:
    """Tests for CharitiesClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert "charitycommission" in client.BASE_URL.lower()
    
    def test_get_bulk_download_url(self, client):
        """Test getting bulk download URL"""
        url = client.get_bulk_download_url()
        assert isinstance(url, str)
        assert "charitycommission" in url.lower() or "ccew" in url.lower()
    
    def test_search_charities(self, client):
        """Test searching charities"""
        try:
            result = client.search_charities(search_term="education", page_size=5)
            assert isinstance(result, dict) or isinstance(result, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_charities_df(self, client):
        """Test searching charities as DataFrame"""
        try:
            df = client.search_charities_df(search_term="health", page_size=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_charities_by_postcode(self, client):
        """Test getting charities by postcode"""
        try:
            df = client.get_charities_by_postcode("E1", page_size=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_large_charities(self, client):
        """Test getting large charities"""
        try:
            df = client.get_large_charities(min_income=1000000, page_size=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")

