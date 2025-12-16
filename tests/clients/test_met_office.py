"""
Tests for Met Office API Client
"""

import pytest
import pandas as pd
from src.clients import MetOfficeClient


@pytest.fixture
def client():
    """Create client instance"""
    return MetOfficeClient()


class TestMetOfficeClient:
    """Tests for MetOfficeClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert "datapoint.metoffice.gov.uk" in client.BASE_URL
    
    def test_get_weather_types(self, client):
        """Test getting weather type codes"""
        types = client.get_weather_types()
        assert isinstance(types, dict)
        assert len(types) > 20
        assert '0' in types
        assert 'Clear night' in types.values()
    
    def test_get_parameter_codes(self, client):
        """Test getting parameter codes"""
        params = client.get_parameter_codes()
        assert isinstance(params, dict)
        assert 'T' in params  # Temperature
        assert 'F' in params  # Feels like
        assert 'W' in params  # Weather type
    
    def test_health_check_no_key(self, client):
        """Test health check without API key"""
        if not client.api_key:
            assert client.health_check() is False
    
    @pytest.mark.skipif(True, reason="Requires API key")
    def test_get_forecast_sites(self, client):
        """Test getting forecast sites"""
        sites = client.get_forecast_sites()
        assert isinstance(sites, list)
    
    @pytest.mark.skipif(True, reason="Requires API key")
    def test_get_forecast(self, client):
        """Test getting forecast"""
        result = client.get_forecast("3772")  # London
        assert isinstance(result, dict)

