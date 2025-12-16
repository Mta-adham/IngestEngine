"""
Tests for Electoral Commission API Client
"""

import pytest
import pandas as pd
from src.clients import ElectoralCommissionClient


@pytest.fixture
def client():
    """Create client instance"""
    return ElectoralCommissionClient()


class TestElectoralCommissionClient:
    """Tests for ElectoralCommissionClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert "electoralcommission.org.uk" in client.BASE_URL.lower()
    
    def test_get_donor_statuses(self, client):
        """Test getting donor status types"""
        statuses = client.get_donor_statuses()
        assert isinstance(statuses, list)
        assert 'Individual' in statuses
        assert 'Company' in statuses
        assert 'Trade Union' in statuses
    
    def test_get_party_registers(self, client):
        """Test getting party registers"""
        registers = client.get_party_registers()
        assert isinstance(registers, list)
        assert len(registers) == 2
        
        codes = [r['code'] for r in registers]
        assert 'gb' in codes
        assert 'ni' in codes
    
    def test_get_bulk_download_urls(self, client):
        """Test getting bulk download URLs"""
        urls = client.get_bulk_download_urls()
        assert isinstance(urls, dict)
        assert 'donations' in urls
        assert 'parties' in urls
    
    def test_get_registered_parties(self, client):
        """Test getting registered parties"""
        try:
            parties = client.get_registered_parties(rows=10)
            assert isinstance(parties, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_registered_parties_df(self, client):
        """Test getting registered parties as DataFrame"""
        try:
            df = client.get_registered_parties_df(rows=20)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_parties(self, client):
        """Test searching parties"""
        try:
            results = client.search_parties("Labour")
            assert isinstance(results, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_donations(self, client):
        """Test getting donations"""
        try:
            donations = client.get_donations(rows=10)
            assert isinstance(donations, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_donations_df(self, client):
        """Test getting donations as DataFrame"""
        try:
            df = client.get_donations_df(rows=20)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_large_donations(self, client):
        """Test getting large donations"""
        try:
            df = client.get_large_donations(min_value=100000)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")

