"""
Police.uk API Client Tests
==========================

Tests for retrieving crime data from Police.uk.
No API key required.
"""

import pytest
from src.clients import PoliceUKClient


class TestPoliceUKClient:
    """Test suite for Police.uk API"""
    
    @pytest.fixture
    def client(self):
        """Create a Police.uk client instance"""
        return PoliceUKClient()
    
    # Test coordinates: Westminster, London
    TEST_LAT = 51.5007
    TEST_LON = -0.1246
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
    
    def test_get_forces(self, client):
        """Test retrieving all police forces"""
        forces = client.get_forces()
        
        assert forces is not None
        assert len(forces) > 40  # UK has ~43 police forces
        
        # Check force structure
        force = forces[0]
        assert "id" in force
        assert "name" in force
    
    def test_get_crime_categories(self, client):
        """Test retrieving crime categories"""
        categories = client.get_crime_categories()
        
        assert categories is not None
        assert len(categories) > 0
        
        # Check category structure
        category = categories[0]
        assert "url" in category
        assert "name" in category
    
    def test_get_street_level_crimes(self, client):
        """Test retrieving street-level crimes for a location"""
        crimes = client.get_street_level_crimes(self.TEST_LAT, self.TEST_LON)
        
        assert crimes is not None
        assert len(crimes) > 0
        
        # Check crime structure
        crime = crimes[0]
        assert "category" in crime
        assert "location" in crime
    
    def test_get_street_level_crimes_with_date(self, client):
        """Test retrieving crimes for a specific month"""
        crimes = client.get_street_level_crimes(
            self.TEST_LAT, 
            self.TEST_LON,
            date="2024-01"
        )
        
        assert crimes is not None
    
    def test_get_crimes_at_location(self, client):
        """Test retrieving crimes at exact location"""
        crimes = client.get_crimes_at_location(self.TEST_LAT, self.TEST_LON)
        
        assert crimes is not None
    
    def test_get_stop_and_search(self, client):
        """Test retrieving stop and search data"""
        stops = client.get_stop_and_search(self.TEST_LAT, self.TEST_LON)
        
        # May be empty in some areas
        assert stops is not None
    
    def test_health_check(self, client):
        """Test API health check"""
        is_healthy = client.health_check()
        assert is_healthy is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

