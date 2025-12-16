"""
Tests for Ofcom API Client
"""

import pytest
import pandas as pd
from src.clients import OfcomClient


@pytest.fixture
def client():
    """Create client instance"""
    return OfcomClient()


class TestOfcomClient:
    """Tests for OfcomClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
    
    def test_health_check(self, client):
        """Test health check"""
        result = client.health_check()
        assert result is True
    
    def test_get_broadband_speed_tiers(self, client):
        """Test getting broadband speed tiers"""
        tiers = client.get_broadband_speed_tiers()
        assert isinstance(tiers, list)
        assert len(tiers) >= 4
        
        # Check structure
        first = tiers[0]
        assert 'tier' in first
        assert 'speed_mbps' in first
        assert 'description' in first
    
    def test_get_mobile_operators(self, client):
        """Test getting mobile operators"""
        operators = client.get_mobile_operators()
        assert isinstance(operators, list)
        assert len(operators) == 4  # EE, Three, O2, Vodafone
        
        names = [op['name'] for op in operators]
        assert 'EE' in names
        assert 'Vodafone' in names
    
    def test_get_spectrum_bands(self, client):
        """Test getting spectrum bands"""
        bands = client.get_spectrum_bands()
        assert isinstance(bands, list)
        assert len(bands) > 5
        
        # Check structure
        first = bands[0]
        assert 'band' in first
        assert 'use' in first
        assert 'operators' in first
    
    def test_get_connected_nations_data_urls(self, client):
        """Test getting data URLs"""
        urls = client.get_connected_nations_data_urls()
        assert isinstance(urls, dict)
        assert 'fixed_broadband' in urls
        assert 'mobile_coverage' in urls
    
    @pytest.mark.skip(reason="Coverage API may require auth")
    def test_get_broadband_coverage(self, client):
        """Test getting broadband coverage"""
        result = client.get_broadband_coverage("SW1A1AA")
        assert isinstance(result, dict)
    
    @pytest.mark.skip(reason="Coverage API may require auth")
    def test_get_mobile_coverage(self, client):
        """Test getting mobile coverage"""
        result = client.get_mobile_coverage("SW1A1AA")
        assert isinstance(result, dict)

