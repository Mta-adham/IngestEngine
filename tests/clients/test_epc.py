"""
EPC API Client Tests
====================

Tests for retrieving Energy Performance Certificate data.
API Key required: Set EPC_EMAIL and EPC_API_KEY in config or env.
"""

import pytest
from src.clients import EPCClient


class TestEPCClient:
    """Test suite for EPC API"""
    
    @pytest.fixture
    def client(self):
        """Create an EPC client instance"""
        return EPCClient()
    
    def test_client_initialization(self, client):
        """Test client initializes with credentials"""
        assert client is not None
        assert client.email is not None
        assert client.api_key is not None
    
    def test_search_domestic_by_postcode(self, client):
        """Test searching domestic EPCs by postcode"""
        result = client.search_domestic(postcode="E1", size=5)
        
        assert result is not None
        assert "rows" in result
        assert len(result["rows"]) > 0
        assert "column-names" in result
    
    def test_search_domestic_by_address(self, client):
        """Test searching domestic EPCs by address"""
        result = client.search_domestic(address="London", size=5)
        
        assert result is not None
        assert "rows" in result
    
    def test_search_domestic_by_local_authority(self, client):
        """Test searching by local authority"""
        # E09000001 = City of London
        result = client.search_domestic(local_authority="E09000001", size=5)
        
        assert result is not None
        assert "rows" in result
    
    def test_search_non_domestic(self, client):
        """Test searching non-domestic EPCs"""
        result = client.search_non_domestic(postcode="EC1", size=5)
        
        assert result is not None
        assert "rows" in result
    
    def test_get_domestic_by_postcode_df(self, client):
        """Test getting domestic EPCs as DataFrame"""
        df = client.get_domestic_by_postcode_df("SW1")
        
        assert df is not None
        assert len(df) > 0
    
    def test_search_with_filters(self, client):
        """Test searching with energy rating filter"""
        result = client.search_domestic(
            postcode="N1",
            energy_band="A",
            size=5
        )
        
        assert result is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

