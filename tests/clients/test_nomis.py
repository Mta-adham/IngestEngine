"""
Tests for NOMIS Labour Market API Client
"""

import pytest
import pandas as pd
from src.clients import NOMISClient


@pytest.fixture
def client():
    """Create client instance"""
    return NOMISClient()


class TestNOMISClient:
    """Tests for NOMISClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert "nomisweb.co.uk" in client.BASE_URL.lower()
    
    def test_health_check(self, client):
        """Test health check"""
        try:
            result = client.health_check()
            assert isinstance(result, bool)
        except Exception:
            pytest.skip("API unavailable")
    
    def test_get_common_datasets(self, client):
        """Test getting common dataset IDs"""
        datasets = client.get_common_datasets()
        assert isinstance(datasets, dict)
        assert 'claimant_count' in datasets
        assert 'earnings' in datasets
        assert 'business_counts' in datasets
    
    def test_get_geography_types(self, client):
        """Test getting geography types"""
        types = client.get_geography_types()
        assert isinstance(types, list)
        assert len(types) > 0
        
        # Check structure
        first = types[0]
        assert 'code' in first
        assert 'name' in first
    
    def test_get_datasets(self, client):
        """Test getting dataset list"""
        try:
            datasets = client.get_datasets()
            assert isinstance(datasets, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_datasets_df(self, client):
        """Test getting datasets as DataFrame"""
        try:
            df = client.get_datasets_df()
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_local_authorities(self, client):
        """Test getting local authorities"""
        try:
            df = client.get_local_authorities()
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_claimant_count(self, client):
        """Test getting claimant count data"""
        try:
            result = client.get_claimant_count("E09000001")  # City of London
            assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"API error: {e}")

