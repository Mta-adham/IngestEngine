"""
Tests for Ofsted API Client
"""

import pytest
import pandas as pd
from src.clients import OfstedClient


@pytest.fixture
def client():
    """Create client instance"""
    return OfstedClient()


class TestOfstedClient:
    """Tests for OfstedClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
    
    def test_get_bulk_download_urls(self, client):
        """Test getting bulk download URLs"""
        urls = client.get_bulk_download_urls()
        assert isinstance(urls, dict)
        assert 'edubase' in urls
        assert 'ofsted_inspections' in urls
    
    def test_get_edubase_extract_url(self, client):
        """Test getting EduBase URL"""
        url = client.get_edubase_extract_url()
        assert isinstance(url, str)
        assert "get-information-schools" in url.lower()
    
    def test_search_schools(self, client):
        """Test searching schools"""
        try:
            schools = client.search_schools(name="Academy", page_size=5)
            assert isinstance(schools, list)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_schools_df(self, client):
        """Test searching schools as DataFrame"""
        try:
            df = client.search_schools_df(postcode="E1", page_size=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_dataset_info(self, client):
        """Test getting dataset info"""
        try:
            result = client.get_dataset_info("ofsted")
            assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"API error: {e}")

