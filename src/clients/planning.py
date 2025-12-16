"""
Planning Data API Client
=========================

Access planning application data across UK local authorities.

API Documentation: https://www.planning.data.gov.uk/

Usage:
    from src.clients import PlanningClient
    
    client = PlanningClient()
    applications = client.search_applications(postcode="SW1")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class PlanningClient(BaseAPIClient):
    """
    Client for Planning Data API.
    
    Access planning applications and decisions.
    """
    
    BASE_URL = "https://www.planning.data.gov.uk/api/v1"
    
    def __init__(self, **kwargs):
        """Initialize Planning client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_datasets()
            return True
        except Exception:
            return False
    
    # ========================================
    # DATASETS
    # ========================================
    
    def get_datasets(self) -> List[Dict]:
        """Get list of available datasets"""
        return self.get('/dataset')
    
    def get_dataset(self, dataset_name: str) -> Dict:
        """Get info about a specific dataset"""
        return self.get(f'/dataset/{dataset_name}')
    
    # ========================================
    # ENTITIES
    # ========================================
    
    def search_entities(
        self,
        dataset: str,
        geometry: Optional[str] = None,
        geometry_relation: str = "intersects",
        point: Optional[str] = None,
        entries: Optional[str] = None,
        field: Optional[str] = None,
        limit: int = 100
    ) -> Dict:
        """
        Search for entities in a dataset.
        
        Args:
            dataset: Dataset name (e.g., 'article-4-direction', 'conservation-area')
            geometry: WKT geometry string
            geometry_relation: intersects, within, contains
            point: Point as "POINT(lon lat)"
            entries: Filter specific entries
            field: Field filter
            limit: Max results
            
        Returns:
            Search results
        """
        params = {
            'dataset': dataset,
            'limit': limit
        }
        
        if geometry:
            params['geometry'] = geometry
            params['geometry_relation'] = geometry_relation
        if point:
            params['point'] = point
        if entries:
            params['entries'] = entries
        if field:
            params['field'] = field
        
        return self.get('/entity', params=params)
    
    def search_entities_df(self, **kwargs) -> pd.DataFrame:
        """Search entities as DataFrame"""
        result = self.search_entities(**kwargs)
        entities = result.get('entities', result) if isinstance(result, dict) else result
        return pd.DataFrame(entities)
    
    def get_entity(self, entity_id: int) -> Dict:
        """Get a specific entity"""
        return self.get(f'/entity/{entity_id}')
    
    # ========================================
    # PLANNING CONSTRAINTS
    # ========================================
    
    def get_conservation_areas(
        self,
        point: Optional[str] = None,
        geometry: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Get conservation areas"""
        return self.search_entities_df(
            dataset='conservation-area',
            point=point,
            geometry=geometry,
            limit=limit
        )
    
    def get_listed_buildings(
        self,
        point: Optional[str] = None,
        geometry: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Get listed buildings"""
        return self.search_entities_df(
            dataset='listed-building',
            point=point,
            geometry=geometry,
            limit=limit
        )
    
    def get_article4_directions(
        self,
        point: Optional[str] = None,
        geometry: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Get Article 4 directions"""
        return self.search_entities_df(
            dataset='article-4-direction',
            point=point,
            geometry=geometry,
            limit=limit
        )
    
    def get_tree_preservation_orders(
        self,
        point: Optional[str] = None,
        geometry: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Get tree preservation orders"""
        return self.search_entities_df(
            dataset='tree-preservation-order',
            point=point,
            geometry=geometry,
            limit=limit
        )
    
    # ========================================
    # LOCATION-BASED QUERIES
    # ========================================
    
    def get_constraints_for_point(
        self,
        lat: float,
        lon: float
    ) -> Dict[str, pd.DataFrame]:
        """
        Get all planning constraints for a point.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dict of constraint types to DataFrames
        """
        point = f"POINT({lon} {lat})"
        
        constraints = {}
        
        datasets = [
            'conservation-area',
            'listed-building',
            'article-4-direction',
            'tree-preservation-order',
            'flood-risk-zone',
            'green-belt',
            'area-of-outstanding-natural-beauty',
            'site-of-special-scientific-interest',
        ]
        
        for dataset in datasets:
            try:
                df = self.search_entities_df(dataset=dataset, point=point, limit=100)
                if not df.empty:
                    constraints[dataset] = df
            except Exception:
                pass
        
        return constraints
    
    def check_planning_constraints(
        self,
        lat: float,
        lon: float
    ) -> Dict[str, bool]:
        """
        Quick check for planning constraints at a point.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dict of constraint types to boolean (present/not)
        """
        constraints = self.get_constraints_for_point(lat, lon)
        
        return {
            'in_conservation_area': 'conservation-area' in constraints,
            'listed_building': 'listed-building' in constraints,
            'article_4': 'article-4-direction' in constraints,
            'tpo': 'tree-preservation-order' in constraints,
            'flood_zone': 'flood-risk-zone' in constraints,
            'green_belt': 'green-belt' in constraints,
            'aonb': 'area-of-outstanding-natural-beauty' in constraints,
            'sssi': 'site-of-special-scientific-interest' in constraints,
        }
    
    # ========================================
    # BULK DATA
    # ========================================
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk planning data"""
        return {
            'all_datasets': 'https://www.planning.data.gov.uk/dataset',
            'local_plans': 'https://www.gov.uk/government/collections/local-plans-examined',
            'brownfield_land': 'https://www.gov.uk/guidance/brownfield-land-registers',
        }

