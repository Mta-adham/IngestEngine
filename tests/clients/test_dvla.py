"""
Tests for DVLA API Client
"""

import pytest
import pandas as pd
from src.clients import DVLAClient


@pytest.fixture
def client():
    """Create client instance"""
    return DVLAClient()


class TestDVLAClient:
    """Tests for DVLAClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert "driver-vehicle-licensing" in client.BASE_URL.lower()
    
    def test_get_fuel_types(self, client):
        """Test getting fuel types"""
        types = client.get_fuel_types()
        assert isinstance(types, list)
        assert 'PETROL' in types
        assert 'DIESEL' in types
        assert 'ELECTRIC' in types
    
    def test_get_tax_status_types(self, client):
        """Test getting tax status types"""
        statuses = client.get_tax_status_types()
        assert isinstance(statuses, list)
        assert 'Taxed' in statuses
        assert 'SORN' in statuses
    
    def test_health_check_no_key(self, client):
        """Test health check without API key"""
        if not client.api_key:
            assert client.health_check() is False
    
    @pytest.mark.skip(reason="Requires API key")
    def test_get_vehicle(self, client):
        """Test getting vehicle data"""
        result = client.get_vehicle("AB12CDE")
        assert isinstance(result, dict)
    
    @pytest.mark.skip(reason="Requires API key")
    def test_get_vehicle_summary(self, client):
        """Test getting vehicle summary"""
        result = client.get_vehicle_summary("AB12CDE")
        assert isinstance(result, dict)
        assert 'registration' in result

