"""
Unified Data Fetcher Tests
==========================

Tests for the unified interface that combines all API clients.
"""

import pytest
from src.clients import UnifiedDataFetcher


class TestUnifiedDataFetcher:
    """Test suite for Unified Data Fetcher"""
    
    @pytest.fixture
    def fetcher(self):
        """Create a UnifiedDataFetcher instance"""
        return UnifiedDataFetcher()
    
    # Test locations
    TEST_POSTCODE = "E1 6AN"
    TEST_LAT = 51.5074
    TEST_LON = -0.1278
    
    def test_fetcher_initialization(self, fetcher):
        """Test fetcher initializes all clients"""
        assert fetcher is not None
        assert len(fetcher.clients) > 0
    
    def test_health_check_all(self, fetcher):
        """Test health check for all clients"""
        health = fetcher.health_check_all()
        
        assert health is not None
        assert isinstance(health, dict)
        # At least some should be healthy
        healthy_count = sum(1 for v in health.values() if v)
        assert healthy_count > 0
    
    def test_get_all_data_for_postcode(self, fetcher):
        """Test fetching all data for a postcode"""
        data = fetcher.get_all_data_for_postcode(
            self.TEST_POSTCODE,
            exclude=["companies_house"]
        )
        
        assert data is not None
        assert "postcode" in data or "geography" in data
        assert "sources" in data
    
    def test_get_all_data_for_location(self, fetcher):
        """Test fetching all data for coordinates"""
        data = fetcher.get_all_data_for_location(
            self.TEST_LAT,
            self.TEST_LON,
            radius_m=500,
            exclude=["companies_house", "epc", "os"]
        )
        
        assert data is not None
        assert "sources" in data
    
    def test_get_data_with_include_filter(self, fetcher):
        """Test fetching specific sources only"""
        data = fetcher.get_all_data_for_location(
            self.TEST_LAT,
            self.TEST_LON,
            include=["police", "ons"]
        )
        
        assert data is not None
        assert "sources" in data
    
    def test_get_transport_data(self, fetcher):
        """Test fetching transport data specifically"""
        if "tfl" in fetcher.clients:
            data = fetcher.get_all_data_for_location(
                self.TEST_LAT,
                self.TEST_LON,
                include=["tfl"]
            )
            
            assert data is not None
    
    def test_get_crime_data(self, fetcher):
        """Test fetching crime data specifically"""
        if "police" in fetcher.clients:
            data = fetcher.get_all_data_for_location(
                self.TEST_LAT,
                self.TEST_LON,
                include=["police"]
            )
            
            assert data is not None
    
    def test_get_environmental_data(self, fetcher):
        """Test fetching environmental data"""
        if "environment_agency" in fetcher.clients:
            data = fetcher.get_all_data_for_location(
                self.TEST_LAT,
                self.TEST_LON,
                include=["environment_agency"]
            )
            
            assert data is not None
    
    def test_get_healthcare_data(self, fetcher):
        """Test fetching healthcare data"""
        if "nhs" in fetcher.clients:
            data = fetcher.get_all_data_for_postcode(
                self.TEST_POSTCODE,
                include=["nhs"]
            )
            
            assert data is not None
    
    def test_exclude_sources(self, fetcher):
        """Test excluding specific sources"""
        data = fetcher.get_all_data_for_location(
            self.TEST_LAT,
            self.TEST_LON,
            exclude=["companies_house", "epc", "os", "wikidata"]
        )
        
        assert data is not None
        sources = data.get("sources", {})
        assert "companies_house" not in sources
    
    def test_data_includes_geography(self, fetcher):
        """Test that location data includes geography info"""
        data = fetcher.get_all_data_for_location(
            self.TEST_LAT,
            self.TEST_LON,
            exclude=["companies_house", "epc", "os"]
        )
        
        # Should have reverse geocoded the location
        assert data.get("postcode") is not None or data.get("geography") is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

