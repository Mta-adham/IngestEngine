"""
Base API Client
===============

Provides common functionality for all API clients:
- Rate limiting
- Retry logic with exponential backoff
- Request/response logging
- Error handling
"""

import requests
import time
import logging
from typing import Optional, Dict, Any, List
from abc import ABC, abstractmethod
import json
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class APIError(Exception):
    """Base exception for API errors"""
    def __init__(self, message: str, status_code: int = None, response: dict = None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)


class RateLimitError(APIError):
    """Rate limit exceeded"""
    pass


class AuthenticationError(APIError):
    """Authentication failed"""
    pass


class BaseAPIClient(ABC):
    """
    Base class for all API clients.
    
    Provides:
    - Rate limiting (configurable requests per minute)
    - Retry logic with exponential backoff
    - Session management
    - Response caching (optional)
    """
    
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        rate_limit_rpm: int = 60,
        max_retries: int = 3,
        timeout: int = 30,
        cache_dir: Optional[str] = None,
        user_agent: str = "IngestEngine/1.0"
    ):
        """
        Initialize base client.
        
        Args:
            base_url: Base URL for API
            api_key: API key (if required)
            rate_limit_rpm: Requests per minute limit
            max_retries: Maximum retry attempts
            timeout: Request timeout in seconds
            cache_dir: Directory for response caching
            user_agent: User agent string
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.rate_limit_rpm = rate_limit_rpm
        self.rate_limit_delay = 60.0 / rate_limit_rpm
        self.max_retries = max_retries
        self.timeout = timeout
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.user_agent = user_agent
        
        self.last_request_time = 0
        self.request_count = 0
        
        # Setup session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'application/json'
        })
        
        # Add auth headers if API key provided
        self._setup_auth()
        
        # Create cache dir if needed
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _setup_auth(self):
        """Setup authentication headers. Override in subclasses."""
        pass
    
    def _rate_limit(self):
        """Enforce rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
        self.request_count += 1
    
    def _get_cache_key(self, endpoint: str, params: dict) -> str:
        """Generate cache key for request"""
        import hashlib
        key_data = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_cached(self, cache_key: str) -> Optional[dict]:
        """Get cached response if available"""
        if not self.cache_dir:
            return None
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            try:
                with open(cache_file) as f:
                    return json.load(f)
            except:
                return None
        return None
    
    def _set_cache(self, cache_key: str, data: dict):
        """Cache response data"""
        if not self.cache_dir:
            return
        cache_file = self.cache_dir / f"{cache_key}.json"
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Failed to cache response: {e}")
    
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Make HTTP request with rate limiting and retries.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (appended to base_url)
            params: Query parameters
            data: Form data
            json_data: JSON body
            headers: Additional headers
            use_cache: Whether to use caching
            
        Returns:
            Response data as dictionary
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        params = params or {}
        
        # Check cache for GET requests
        if method.upper() == 'GET' and use_cache:
            cache_key = self._get_cache_key(endpoint, params)
            cached = self._get_cached(cache_key)
            if cached:
                logger.debug(f"Cache hit: {endpoint}")
                return cached
        
        # Rate limiting
        self._rate_limit()
        
        # Merge headers
        request_headers = dict(self.session.headers)
        if headers:
            request_headers.update(headers)
        
        # Retry loop
        last_error = None
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Request {method} {url} (attempt {attempt + 1})")
                
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    json=json_data,
                    headers=request_headers,
                    timeout=self.timeout
                )
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                # Handle auth errors
                if response.status_code in (401, 403):
                    raise AuthenticationError(
                        f"Authentication failed: {response.text}",
                        status_code=response.status_code
                    )
                
                # Handle other errors
                if response.status_code >= 400:
                    raise APIError(
                        f"API error: {response.text}",
                        status_code=response.status_code
                    )
                
                # Parse response
                try:
                    result = response.json()
                except json.JSONDecodeError:
                    result = {'raw_response': response.text}
                
                # Cache successful GET responses
                if method.upper() == 'GET' and use_cache:
                    self._set_cache(cache_key, result)
                
                return result
                
            except requests.exceptions.Timeout:
                last_error = APIError(f"Request timeout after {self.timeout}s")
                logger.warning(f"Timeout on attempt {attempt + 1}")
                
            except requests.exceptions.ConnectionError as e:
                last_error = APIError(f"Connection error: {e}")
                logger.warning(f"Connection error on attempt {attempt + 1}")
            
            # Exponential backoff
            if attempt < self.max_retries - 1:
                sleep_time = 2 ** attempt
                logger.info(f"Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
        
        raise last_error or APIError("Request failed after all retries")
    
    def get(self, endpoint: str, params: Optional[Dict] = None, **kwargs) -> Dict:
        """Make GET request"""
        return self._request('GET', endpoint, params=params, **kwargs)
    
    def post(self, endpoint: str, data: Optional[Dict] = None, 
             json_data: Optional[Dict] = None, **kwargs) -> Dict:
        """Make POST request"""
        return self._request('POST', endpoint, data=data, json_data=json_data, **kwargs)
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check if API is available. Override in subclasses."""
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics"""
        return {
            'base_url': self.base_url,
            'request_count': self.request_count,
            'rate_limit_rpm': self.rate_limit_rpm,
            'has_api_key': bool(self.api_key),
            'cache_enabled': bool(self.cache_dir)
        }

