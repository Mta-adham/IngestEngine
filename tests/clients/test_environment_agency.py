"""
Environment Agency API Client Tests
===================================

Tests for retrieving environmental data.
No API key required.
"""

import pytest
from src.clients import EnvironmentAgencyClient


class TestEnvironmentAgencyClient:
    """Test suite for Environment Agency API"""
    
    @pytest.fixture
    def client(self):
        """Create an Environment Agency client instance"""
        return EnvironmentAgencyClient()
    
    # Test coordinates: London
    TEST_LAT = 51.5074
    TEST_LON = -0.1278
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
    
    def test_get_current_flood_warnings(self, client):
        """Test retrieving current flood warnings"""
        warnings = client.get_current_flood_warnings()
        
        assert warnings is not None
        # May be empty if no current warnings
        assert isinstance(warnings, list)
    
    def test_get_stations(self, client):
        """Test retrieving monitoring stations"""
        stations = client.get_stations()
        
        assert stations is not None
        assert len(stations) > 0
    
    def test_get_stations_near_location(self, client):
        """Test retrieving stations near coordinates"""
        stations = client.get_stations(
            lat=self.TEST_LAT,
            lon=self.TEST_LON,
            dist=20  # 20km radius
        )
        
        assert stations is not None
        assert len(stations) > 0
    
    def test_get_flood_areas(self, client):
        """Test retrieving flood areas"""
        areas = client.get_flood_areas()
        
        assert areas is not None
        assert len(areas) > 0
    
    def test_get_flood_risk_for_location(self, client):
        """Test getting flood risk for a location"""
        risk = client.get_flood_risk_for_location(self.TEST_LAT, self.TEST_LON)
        
        assert risk is not None
    
    def test_get_stations_df(self, client):
        """Test getting stations as DataFrame"""
        df = client.get_stations_df(lat=self.TEST_LAT, lon=self.TEST_LON, dist=10)
        
        assert df is not None
        assert len(df) > 0
    
    def test_get_flood_warnings_df(self, client):
        """Test getting flood warnings as DataFrame"""
        df = client.get_flood_warnings_df()
        
        assert df is not None
    
    def test_health_check(self, client):
        """Test API health check"""
        is_healthy = client.health_check()
        assert is_healthy is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

