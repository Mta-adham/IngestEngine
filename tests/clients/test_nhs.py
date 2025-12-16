"""
NHS ODS API Client Tests
========================

Tests for retrieving NHS organisation data.
No API key required.
"""

import pytest
from src.clients import NHSClient


class TestNHSClient:
    """Test suite for NHS ODS API"""
    
    @pytest.fixture
    def client(self):
        """Create an NHS client instance"""
        return NHSClient()
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
    
    def test_get_organisation_by_ods_code(self, client):
        """Test retrieving organisation by ODS code"""
        # RRV = University College London Hospitals NHS Foundation Trust
        org = client.get_organisation("RRV")
        
        assert org is not None
    
    def test_search_organisations(self, client):
        """Test searching for organisations"""
        result = client.search_organisations(
            name="Hospital",
            status="Active",
            limit=10
        )
        
        assert result is not None
    
    def test_get_gp_practices(self, client):
        """Test retrieving GP practices by postcode"""
        gps = client.get_gp_practices(postcode="E1", limit=10)
        
        assert gps is not None
    
    def test_get_gp_practices_df(self, client):
        """Test getting GP practices as DataFrame"""
        df = client.get_gp_practices_df(postcode="SW1", limit=20)
        
        assert df is not None
    
    def test_get_hospitals(self, client):
        """Test retrieving hospitals"""
        hospitals = client.get_hospitals(limit=10)
        
        assert hospitals is not None
    
    def test_get_hospitals_df(self, client):
        """Test getting hospitals as DataFrame"""
        df = client.get_hospitals_df(limit=20)
        
        assert df is not None
    
    def test_get_pharmacies(self, client):
        """Test retrieving pharmacies by postcode"""
        pharmacies = client.get_pharmacies(postcode="E1", limit=10)
        
        assert pharmacies is not None
    
    def test_get_pharmacies_df(self, client):
        """Test getting pharmacies as DataFrame"""
        df = client.get_pharmacies_df(postcode="N1", limit=20)
        
        assert df is not None
    
    def test_search_by_role(self, client):
        """Test searching by role code"""
        result = client.search_organisations(
            role_id="RO76",
            limit=10
        )
        
        assert result is not None
    
    def test_health_check(self, client):
        """Test API health check"""
        is_healthy = client.health_check()
        assert is_healthy is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

