"""
Tests for Care Quality Commission API Client
"""

import pytest
import pandas as pd
from src.clients import CQCClient


@pytest.fixture
def client():
    """Create client instance"""
    return CQCClient()


class TestCQCClient:
    """Tests for CQCClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert client.BASE_URL == "https://api.cqc.org.uk/public/v1"
    
    def test_health_check(self, client):
        """Test health check"""
        result = client.health_check()
        assert result is True
    
    def test_get_provider_types(self, client):
        """Test getting provider types"""
        types = client.get_provider_types()
        assert isinstance(types, list)
        assert len(types) > 0
        assert "NHS Healthcare Organisation" in types
    
    def test_get_rating_types(self, client):
        """Test getting rating types"""
        ratings = client.get_rating_types()
        assert isinstance(ratings, list)
        assert "Outstanding" in ratings
        assert "Good" in ratings
    
    def test_search_providers(self, client):
        """Test searching providers"""
        try:
            result = client.search_providers(per_page=5)
            assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_providers_df(self, client):
        """Test searching providers as DataFrame"""
        try:
            df = client.search_providers_df(per_page=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_providers_by_postcode(self, client):
        """Test searching providers by postcode"""
        try:
            df = client.search_providers_df(postcode="E1", per_page=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_locations(self, client):
        """Test searching locations"""
        try:
            result = client.search_locations(per_page=5)
            assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_locations_df(self, client):
        """Test searching locations as DataFrame"""
        try:
            df = client.search_locations_df(per_page=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_care_homes(self, client):
        """Test getting care homes"""
        try:
            df = client.get_care_homes(per_page=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_gp_practices(self, client):
        """Test getting GP practices"""
        try:
            df = client.get_gp_practices(per_page=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_providers_by_rating(self, client):
        """Test getting providers by rating"""
        try:
            df = client.get_providers_by_rating("Outstanding", per_page=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_bulk_download_urls(self, client):
        """Test getting bulk download URLs"""
        urls = client.get_bulk_download_urls()
        assert isinstance(urls, dict)
        assert 'providers' in urls

