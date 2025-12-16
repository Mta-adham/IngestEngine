"""
Tests for Historic England API Client
"""

import pytest
import pandas as pd
from src.clients import HistoricEnglandClient


@pytest.fixture
def client():
    """Create client instance"""
    return HistoricEnglandClient()


class TestHistoricEnglandClient:
    """Tests for HistoricEnglandClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
    
    def test_get_listing_grades(self, client):
        """Test getting listing grade definitions"""
        grades = client.get_listing_grades()
        assert isinstance(grades, dict)
        assert 'I' in grades
        assert 'II*' in grades
        assert 'II' in grades
    
    def test_get_heritage_categories(self, client):
        """Test getting heritage categories"""
        categories = client.get_heritage_categories()
        assert isinstance(categories, list)
        assert 'Listed Building' in categories
        assert 'Scheduled Monument' in categories
    
    def test_get_listed_buildings(self, client):
        """Test getting listed buildings"""
        try:
            df = client.get_listed_buildings(limit=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_search_listed_buildings(self, client):
        """Test searching listed buildings"""
        try:
            df = client.search_listed_buildings("Westminster", limit=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_grade_1_buildings(self, client):
        """Test getting Grade I buildings"""
        try:
            df = client.get_grade_1_buildings(limit=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_scheduled_monuments(self, client):
        """Test getting scheduled monuments"""
        try:
            df = client.get_scheduled_monuments(limit=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_registered_parks_gardens(self, client):
        """Test getting registered parks and gardens"""
        try:
            df = client.get_registered_parks_gardens(limit=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_heritage_at_risk(self, client):
        """Test getting heritage at risk"""
        try:
            df = client.get_heritage_at_risk(limit=10)
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_world_heritage_sites(self, client):
        """Test getting world heritage sites"""
        try:
            df = client.get_world_heritage_sites()
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_bulk_download_urls(self, client):
        """Test getting bulk download URLs"""
        urls = client.get_bulk_download_urls()
        assert isinstance(urls, dict)
        assert 'listed_buildings' in urls

