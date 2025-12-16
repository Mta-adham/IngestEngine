"""
Pytest Configuration
====================

Shared fixtures and configuration for all tests.
"""

import pytest
import warnings

# Suppress warnings during tests
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)


def pytest_configure(config):
    """Configure pytest markers"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests that require external APIs"
    )


@pytest.fixture(scope="session")
def test_postcode():
    """Standard test postcode"""
    return "E1 6AN"


@pytest.fixture(scope="session")
def test_coordinates():
    """Standard test coordinates (Westminster)"""
    return {"lat": 51.5007, "lon": -0.1246}


@pytest.fixture(scope="session")
def test_company_number():
    """Standard test company number (Tesco PLC)"""
    return "00445790"

