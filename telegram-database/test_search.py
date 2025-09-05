#!/usr/bin/env python3
"""
Test script for the improved search engine
"""

import logging
import sys
import os

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_search_engine():
    """Test the search engine functionality"""
    try:
        from search_engine import SearchEngine
        
        logging.info("🧪 Testing Search Engine...")
        
        # Initialize search engine
        search_engine = SearchEngine()
        
        # Test search for a city
        city = "Mumbai"
        logging.info(f"🔍 Searching for programming groups in {city}...")
        
        results = search_engine.search_programming_groups(city)
        
        if results:
            logging.info(f"✅ Found {len(results)} results for {city}")
            for i, result in enumerate(results[:3], 1):  # Show first 3 results
                logging.info(f"  {i}. {result.get('title', 'No title')}")
                logging.info(f"     URL: {result.get('url', 'No URL')}")
                logging.info(f"     Source: {result.get('source', 'Unknown')}")
        else:
            logging.warning(f"⚠️ No results found for {city}")
        
        # Test fallback search
        logging.info("🔄 Testing fallback search...")
        fallback_results = search_engine._fallback_search(city)
        if fallback_results:
            logging.info(f"✅ Fallback search found {len(fallback_results)} results")
        else:
            logging.info("ℹ️ Fallback search returned no results")
        
        logging.info("🎉 Search engine test completed!")
        
    except Exception as e:
        logging.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_search_engine() 