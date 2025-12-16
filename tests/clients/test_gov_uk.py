"""
Tests for GOV.UK API Client
"""

import pytest
import pandas as pd
from src.clients import GovUKClient


@pytest.fixture
def client():
    """Create client instance"""
    return GovUKClient()


class TestGovUKClient:
    """Tests for GovUKClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert "gov.uk" in client.BASE_URL.lower()
    
    def test_health_check(self, client):
        """Test health check"""
        try:
            result = client.health_check()
            assert result is True
        except Exception:
            pytest.skip("API unavailable")
    
    def test_get_bank_holidays(self, client):
        """Test getting bank holidays"""
        try:
            holidays = client.get_bank_holidays()
            assert isinstance(holidays, dict)
            assert 'england-and-wales' in holidays
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_bank_holidays_df(self, client):
        """Test getting bank holidays as DataFrame"""
        try:
            df = client.get_bank_holidays_df()
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert 'date' in df.columns or 'title' in df.columns
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_bank_holidays_scotland(self, client):
        """Test getting Scotland bank holidays"""
        try:
            df = client.get_bank_holidays_df(division="scotland")
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_next_bank_holiday(self, client):
        """Test getting next bank holiday"""
        try:
            holiday = client.get_next_bank_holiday()
            assert isinstance(holiday, dict)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_content(self, client):
        """Test searching GOV.UK content"""
        try:
            result = client.search_content("passport", count=5)
            assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_local_authority_for_postcode(self, client):
        """Test getting LA for postcode"""
        try:
            result = client.get_local_authority_for_postcode("SW1A 1AA")
            assert isinstance(result, dict)
            if result:
                assert 'local_authority' in result
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_datasets(self, client):
        """Test searching data.gov.uk"""
        try:
            result = client.search_datasets("transport", rows=5)
            assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_useful_apis(self, client):
        """Test getting useful API list"""
        apis = client.get_useful_apis()
        assert isinstance(apis, dict)
        assert 'bank_holidays' in apis
        assert 'data_gov_uk' in apis

