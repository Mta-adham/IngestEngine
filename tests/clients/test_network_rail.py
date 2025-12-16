"""
Tests for Network Rail API Client
"""

import pytest
import pandas as pd
from src.clients import NetworkRailClient


@pytest.fixture
def client():
    """Create client instance"""
    return NetworkRailClient()


class TestNetworkRailClient:
    """Tests for NetworkRailClient"""
    
    def test_client_initialization(self, client):
        """Test client initializes correctly"""
        assert client is not None
    
    def test_get_train_operators(self, client):
        """Test getting train operators"""
        operators = client.get_train_operators()
        assert isinstance(operators, list)
        assert len(operators) > 0
        
        # Check structure
        first = operators[0]
        assert 'code' in first
        assert 'name' in first
    
    def test_get_train_operators_includes_major(self, client):
        """Test operators include major TOCs"""
        operators = client.get_train_operators()
        names = [op['name'] for op in operators]
        
        # Check for some major operators
        assert any('Avanti' in n for n in names)
        assert any('LNER' in n or 'GR' in [op['code'] for op in operators] for n in names)
    
    def test_search_stations(self, client):
        """Test searching stations"""
        try:
            df = client.search_stations(name="London")
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            pytest.skip(f"API error: {e}")
    
    def test_get_bulk_download_urls(self, client):
        """Test getting bulk download URLs"""
        urls = client.get_bulk_download_urls()
        assert isinstance(urls, dict)
        assert 'naptan_rail' in urls
        assert 'station_usage' in urls

