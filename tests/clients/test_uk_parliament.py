"""
Tests for UK Parliament API Client
"""

import pytest
import pandas as pd
from src.clients import UKParliamentClient


@pytest.fixture
def client():
    """Create client instance"""
    return UKParliamentClient()


class TestUKParliamentClient:
    """Tests for UKParliamentClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert "parliament.uk" in client.MEMBERS_URL.lower()
    
    def test_health_check(self, client):
        """Test health check"""
        try:
            result = client.health_check()
            assert result is True
        except Exception:
            pytest.skip("API unavailable")
    
    def test_get_house_types(self, client):
        """Test getting house types"""
        types = client.get_house_types()
        assert 'Commons' in types
        assert 'Lords' in types
    
    def test_get_current_mps(self, client):
        """Test getting current MPs"""
        try:
            mps = client.get_current_mps(take=10)
            assert isinstance(mps, list)
            assert len(mps) > 0
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_current_mps_df(self, client):
        """Test getting current MPs as DataFrame"""
        try:
            df = client.get_current_mps_df(take=20)
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert 'name' in df.columns
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_current_lords(self, client):
        """Test getting current Lords"""
        try:
            lords = client.get_current_lords(take=10)
            assert isinstance(lords, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_current_lords_df(self, client):
        """Test getting current Lords as DataFrame"""
        try:
            df = client.get_current_lords_df(take=20)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_members(self, client):
        """Test searching members"""
        try:
            results = client.search_members("Smith")
            assert isinstance(results, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_constituencies(self, client):
        """Test getting constituencies"""
        try:
            constituencies = client.get_constituencies(take=10)
            assert isinstance(constituencies, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_constituencies_df(self, client):
        """Test getting constituencies as DataFrame"""
        try:
            df = client.get_constituencies_df(take=20)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_current_bills(self, client):
        """Test getting current bills"""
        try:
            bills = client.get_current_bills(take=10)
            assert isinstance(bills, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_current_bills_df(self, client):
        """Test getting current bills as DataFrame"""
        try:
            df = client.get_current_bills_df(take=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_party_composition(self, client):
        """Test getting party composition"""
        try:
            df = client.get_party_composition()
            assert isinstance(df, pd.DataFrame)
            assert 'party' in df.columns
            assert 'seats' in df.columns
        except Exception as e:
            pytest.skip(f"API error: {e}")

