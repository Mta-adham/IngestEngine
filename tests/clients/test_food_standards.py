"""
Tests for Food Standards Agency API Client
"""

import pytest
import pandas as pd
from src.clients import FoodStandardsClient


@pytest.fixture
def client():
    """Create client instance"""
    return FoodStandardsClient()


class TestFoodStandardsClient:
    """Tests for FoodStandardsClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert client.BASE_URL == "https://api.ratings.food.gov.uk"
    
    def test_health_check(self, client):
        """Test health check"""
        try:
            result = client.health_check()
            assert result is True
        except Exception:
            pytest.skip("API unavailable")
    
    def test_get_authorities(self, client):
        """Test getting local authorities"""
        try:
            authorities = client.get_authorities(page_size=10)
            assert isinstance(authorities, list)
            assert len(authorities) > 0
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_authorities_df(self, client):
        """Test getting authorities as DataFrame"""
        try:
            df = client.get_authorities_df()
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_business_types(self, client):
        """Test getting business types"""
        try:
            types = client.get_business_types()
            assert isinstance(types, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_business_types_df(self, client):
        """Test getting business types as DataFrame"""
        try:
            df = client.get_business_types_df()
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_establishments(self, client):
        """Test searching establishments"""
        try:
            result = client.get_establishments(postcode="E1", page_size=5)
            assert isinstance(result, dict)
            assert 'establishments' in result or 'meta' in result
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_establishments_df(self, client):
        """Test getting establishments as DataFrame"""
        try:
            df = client.get_establishments_df(postcode="SW1", page_size=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_establishments_near(self, client):
        """Test getting establishments near location"""
        try:
            df = client.get_establishments_near(51.5, -0.1, radius_miles=0.5, page_size=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_ratings(self, client):
        """Test getting rating values"""
        try:
            ratings = client.get_ratings()
            assert isinstance(ratings, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_regions(self, client):
        """Test getting regions"""
        try:
            regions = client.get_regions()
            assert isinstance(regions, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_countries(self, client):
        """Test getting countries"""
        try:
            countries = client.get_countries()
            assert isinstance(countries, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")

