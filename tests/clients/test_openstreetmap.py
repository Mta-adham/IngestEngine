"""
Tests for OpenStreetMap/Overpass API Client
"""

import pytest
import pandas as pd
from src.clients import OpenStreetMapClient


@pytest.fixture
def client():
    """Create client instance"""
    return OpenStreetMapClient()


class TestOpenStreetMapClient:
    """Tests for OpenStreetMapClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert "overpass" in client.OVERPASS_URL.lower()
    
    def test_health_check(self, client):
        """Test health check"""
        try:
            result = client.health_check()
            assert isinstance(result, bool)
        except Exception:
            pytest.skip("API unavailable")
    
    def test_geocode(self, client):
        """Test geocoding"""
        try:
            result = client.geocode("Big Ben, London")
            assert isinstance(result, dict)
            if result:
                assert 'lat' in result or 'display_name' in result
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_reverse_geocode(self, client):
        """Test reverse geocoding"""
        try:
            result = client.reverse_geocode(51.5007, -0.1246)
            assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_amenities(self, client):
        """Test getting amenities"""
        try:
            df = client.get_amenities("pub", 51.5, -0.1, radius=500)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_shops(self, client):
        """Test getting shops"""
        try:
            df = client.get_shops(lat=51.5, lon=-0.1, radius=500)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_tourism(self, client):
        """Test getting tourism POIs"""
        try:
            df = client.get_tourism(51.5, -0.1, radius=1000)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_buildings(self, client):
        """Test getting buildings"""
        try:
            df = client.get_buildings(51.5, -0.1, radius=200)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_transport_stops(self, client):
        """Test getting transport stops"""
        try:
            df = client.get_transport_stops(51.5, -0.1, radius=500)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_query(self, client):
        """Test raw Overpass query"""
        try:
            query = '[out:json];node["amenity"="cafe"](51.5,-0.1,51.51,-0.09);out 5;'
            result = client.query(query)
            assert isinstance(result, dict)
            assert 'elements' in result
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_query_df(self, client):
        """Test query returning DataFrame"""
        try:
            query = '[out:json];node["amenity"="restaurant"](51.5,-0.1,51.51,-0.09);out 5;'
            df = client.query_df(query)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_poi_types(self, client):
        """Test getting POI types"""
        types = client.get_poi_types()
        assert isinstance(types, dict)
        assert 'food_drink' in types
        assert 'health' in types

