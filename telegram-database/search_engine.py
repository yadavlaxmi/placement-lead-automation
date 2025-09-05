import requests
import logging
import time
from typing import List, Dict, Any
import config

class SearchEngine:
    def __init__(self):
        self.exa_api_key = config.EXA_API_KEY
        self.lookup_api_key = config.LOOKUP_API_KEY
        self.tavily_api_key = config.TAVILY_API_KEY
        
        # Track API usage to avoid rate limits
        self.api_calls = {
            'exa': {'last_call': 0, 'calls_per_minute': 8},  # Reduced from 10 to 8
            'lookup': {'last_call': 0, 'calls_per_minute': 15},  # Reduced from 20 to 15
            'tavily': {'last_call': 0, 'calls_per_minute': 20}   # Reduced from 30 to 20
        }
        
        # Track API health
        self.api_health = {
            'exa': True,
            'lookup': True,
            'tavily': True
        }
    
    def _rate_limit_check(self, api_name: str) -> bool:
        """Check if we can make an API call without hitting rate limits"""
        current_time = time.time()
        last_call = self.api_calls[api_name]['last_call']
        calls_per_minute = self.api_calls[api_name]['calls_per_minute']
        
        if current_time - last_call < (60 / calls_per_minute):
            return False
        
        self.api_calls[api_name]['last_call'] = current_time
        return True
    
    def _mark_api_unhealthy(self, api_name: str):
        """Mark an API as unhealthy after repeated failures"""
        self.api_health[api_name] = False
        logging.warning(f"Marked {api_name} API as unhealthy due to repeated failures")
    
    def _is_api_healthy(self, api_name: str) -> bool:
        """Check if an API is healthy"""
        return self.api_health[api_name]
    
    def search_exa(self, query: str) -> List[Dict[str, Any]]:
        """Search using Exa.ai API"""
        if not self._rate_limit_check('exa'):
            logging.warning(f"Rate limit reached for Exa API, skipping query: {query}")
            return []
            
        try:
            headers = {"Authorization": f"Bearer {self.exa_api_key}"}
            response = requests.post(
                "https://api.exa.ai/search",
                headers=headers,
                json={"query": query, "numResults": 15, "includeDomains": ["t.me", "telegram.me"]},
                timeout=30
            )
            if response.status_code == 200:
                results = response.json().get("results", [])
                logging.info(f"Exa search successful for '{query}': {len(results)} results")
                return results
            else:
                logging.error(f"Exa API error: {response.status_code} - {response.text}")
        except Exception as e:
            logging.error(f"Exa search error: {e}")
        return []
    
    def search_lookup(self, query: str) -> List[Dict[str, Any]]:
        """Search using Lookup.so API"""
        if not self._rate_limit_check('lookup'):
            logging.warning(f"Rate limit reached for Lookup API, skipping query: {query}")
            return []
            
        try:
            headers = {"Authorization": f"Bearer {self.lookup_api_key}"}
            response = requests.get(
                f"https://lookup.so/api/search?q={query}&limit=15",
                headers=headers,
                timeout=30
            )
            if response.status_code == 200:
                results = response.json().get("results", [])
                logging.info(f"Lookup search successful for '{query}': {len(results)} results")
                return results
            else:
                logging.error(f"Lookup API error: {response.status_code} - {response.text}")
        except Exception as e:
            logging.error(f"Lookup search error: {e}")
        return []
    
    def search_tavily(self, query: str) -> List[Dict[str, Any]]:
        """Search using Tavily API"""
        if not self._rate_limit_check('tavily'):
            logging.warning(f"Rate limit reached for Tavily API, skipping query: {query}")
            return []
            
        try:
            headers = {"Authorization": f"Bearer {self.tavily_api_key}"}
            response = requests.post(
                "https://api.tavily.com/search",
                headers=headers,
                json={"query": query, "search_depth": "basic", "max_results": 15},
                timeout=30
            )
            if response.status_code == 200:
                results = response.json().get("results", [])
                logging.info(f"Tavily search successful for '{query}': {len(results)} results")
                return results
            else:
                logging.error(f"Tavily API error: {response.status_code} - {response.text}")
        except Exception as e:
            logging.error(f"Tavily search error: {e}")
        return []
    
    def search_programming_groups(self, city: str, language: str = None) -> List[Dict[str, Any]]:
        """Search for programming groups in a specific city"""
        results = []
        
        # Create more diverse search queries
        queries = [
            f"telegram programming group {city}",
            f"coding jobs {city} telegram",
            f"developer community {city}",
            f"programming jobs {city}",
            f"tech jobs {city} telegram"
        ]
        
        if language:
            queries.extend([
                f"{language} developer {city} telegram",
                f"{language} programming {city} group"
            ])
        
        # Search across working APIs only
        for query in queries:
            try:
                # Only search with healthy APIs
                if self._is_api_healthy('exa'):
                    exa_results = self.search_exa(query)
                    if exa_results:
                        results.extend(exa_results)
                        logging.info(f"Exa found {len(exa_results)} results for: {query}")
                
                # Skip unhealthy APIs to avoid wasting time
                if self._is_api_healthy('lookup'):
                    lookup_results = self.search_lookup(query)
                    if lookup_results:
                        results.extend(lookup_results)
                        logging.info(f"Lookup found {len(lookup_results)} results for: {query}")
                
                if self._is_api_healthy('tavily'):
                    tavily_results = self.search_tavily(query)
                    if tavily_results:
                        results.extend(tavily_results)
                        logging.info(f"Tavily found {len(tavily_results)} results for: {query}")
                
                # Rate limiting between queries
                time.sleep(3)  # Increased from 1 to 3 seconds
                
            except Exception as e:
                logging.error(f"Error searching with query '{query}': {e}")
                continue
        
        # Filter and deduplicate results
        filtered_results = self._filter_telegram_results(results)
        logging.info(f"Found {len(filtered_results)} unique Telegram groups for {city}")
        return filtered_results[:config.MAX_GROUPS_PER_CITY]
    
    def _filter_telegram_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter results to only include Telegram-related content"""
        telegram_results = []
        seen_urls = set()
        
        for result in results:
            url = result.get("url", "")
            if "t.me/" in url or "telegram.me/" in url:
                if url not in seen_urls:
                    seen_urls.add(url)
                    telegram_results.append({
                        "title": result.get("title", ""),
                        "url": url,
                        "description": result.get("description", ""),
                        "source": result.get("source", "")
                    })
        
        return telegram_results 