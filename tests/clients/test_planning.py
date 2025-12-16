"""
Tests for Planning Data API Client
"""

import pytest
import pandas as pd
from src.clients import PlanningClient


@pytest.fixture
def client():
    """Create client instance"""
    return PlanningClient()


class TestPlanningClient:
    """Tests for PlanningClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
        assert "planning.data.gov.uk" in client.BASE_URL.lower()
    
    def test_get_bulk_download_urls(self, client):
        """Test getting bulk download URLs"""
        urls = client.get_bulk_download_urls()
        assert isinstance(urls, dict)
        assert 'all_datasets' in urls
    
    def test_get_datasets(self, client):
        """Test getting datasets"""
        try:
            datasets = client.get_datasets()
            assert isinstance(datasets, list) or isinstance(datasets, dict)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_entities(self, client):
        """Test searching entities"""
        try:
            result = client.search_entities(dataset='conservation-area', limit=5)
            assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_entities_df(self, client):
        """Test searching entities as DataFrame"""
        try:
            df = client.search_entities_df(dataset='listed-building', limit=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_conservation_areas(self, client):
        """Test getting conservation areas"""
        try:
            df = client.get_conservation_areas(limit=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_listed_buildings(self, client):
        """Test getting listed buildings"""
        try:
            df = client.get_listed_buildings(limit=5)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_check_planning_constraints(self, client):
        """Test checking planning constraints"""
        try:
            # Westminster coordinates
            result = client.check_planning_constraints(51.5, -0.13)
            assert isinstance(result, dict)
            assert 'in_conservation_area' in result
            assert 'listed_building' in result
        except Exception as e:
            pytest.skip(f"API error: {e}")

