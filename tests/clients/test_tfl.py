"""
TfL API Client Tests
====================

Tests for retrieving transport data from Transport for London.
No API key required (but rate limited without one).
"""

import pytest
from src.clients import TfLClient


class TestTfLClient:
    """Test suite for TfL API"""
    
    @pytest.fixture
    def client(self):
        """Create a TfL client instance"""
        return TfLClient()
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
    
    def test_get_tube_stations(self, client):
        """Test retrieving tube stations"""
        stations = client.get_tube_stations()
        
        assert stations is not None
        assert len(stations) > 0
    
    def test_get_bike_points(self, client):
        """Test retrieving Santander bike points"""
        df = client.get_bike_points()
        
        assert df is not None
        assert len(df) > 0
    
    def test_get_line_status(self, client):
        """Test getting tube line status"""
        # Get status for a specific line
        status = client.get_line_status("victoria")
        
        assert status is not None
    
    def test_plan_journey(self, client):
        """Test journey planning"""
        # From Westminster to King's Cross
        journey = client.plan_journey(
            from_location="51.501,-0.125",
            to_location="51.530,-0.124"
        )
        
        assert journey is not None
    
    def test_search_stop_points(self, client):
        """Test searching for stop points"""
        stops = client.search_stop_points("Westminster", modes=["tube"])
        
        assert stops is not None
        assert len(stops) > 0
    
    def test_get_bus_stops(self, client):
        """Test retrieving bus stops for a specific route"""
        # Get bus stops for route 73 (a common London bus)
        stops = client.get_bus_stops(line_id="73")
        
        assert stops is not None
    
    def test_health_check(self, client):
        """Test API health check"""
        is_healthy = client.health_check()
        assert is_healthy is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

