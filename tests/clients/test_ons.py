"""
ONS/Postcodes.io API Client Tests
=================================

Tests for retrieving geographic and statistical data.
No API key required.
"""

import pytest
from src.clients import ONSClient


class TestONSClient:
    """Test suite for ONS/Postcodes.io API"""
    
    @pytest.fixture
    def client(self):
        """Create an ONS client instance"""
        return ONSClient()
    
    # Test postcodes
    TEST_POSTCODE = "SW1A 1AA"  # Buckingham Palace
    TEST_POSTCODE_2 = "EC1A 1BB"  # City of London
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
    
    def test_get_postcode_lookup(self, client):
        """Test looking up a postcode"""
        result = client.get_postcode_lookup(self.TEST_POSTCODE)
        
        assert result is not None
        assert "postcode" in result
        assert "latitude" in result
        assert "longitude" in result
    
    def test_get_postcode_lookup_with_details(self, client):
        """Test postcode lookup returns administrative geography"""
        result = client.get_postcode_lookup(self.TEST_POSTCODE)
        
        assert result is not None
        # Should contain admin district (borough/council)
        assert "admin_district" in result or "lad_name" in result
    
    def test_bulk_postcode_lookup(self, client):
        """Test looking up multiple postcodes"""
        postcodes = [self.TEST_POSTCODE, self.TEST_POSTCODE_2, "E1 6AN"]
        
        results = client.bulk_postcode_lookup(postcodes)
        
        assert results is not None
        assert len(results) == 3
    
    def test_health_check(self, client):
        """Test API health check"""
        is_healthy = client.health_check()
        assert is_healthy is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

