"""
OS DataHub API Client Tests
===========================

Tests for retrieving Ordnance Survey data.
API Key required: Set OS_API_KEY in config or env.
"""

import pytest
from src.clients import OSDataHubClient


class TestOSDataHubClient:
    """Test suite for OS DataHub API"""
    
    @pytest.fixture
    def client(self):
        """Create an OS DataHub client instance"""
        return OSDataHubClient()
    
    def test_client_initialization(self, client):
        """Test client initializes with API key"""
        assert client is not None
        assert client.api_key is not None
    
    def test_search_names(self, client):
        """Test searching for place names"""
        results = client.search_names("London", limit=5)
        
        assert results is not None
        assert len(results) > 0
    
    def test_search_places(self, client):
        """Test searching for places (requires Places API access)"""
        try:
            results = client.search_places(query="10 Downing Street")
            assert results is not None
        except Exception as e:
            # Places API may not be enabled for all API keys
            if "Invalid ApiKey" in str(e) or "Authentication" in str(e):
                pytest.skip("OS Places API not enabled for this key")
    
    def test_health_check(self, client):
        """Test API health check"""
        is_healthy = client.health_check()
        assert is_healthy is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

