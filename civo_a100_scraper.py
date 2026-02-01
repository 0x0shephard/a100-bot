#!/usr/bin/env python3
"""
Civo A100 GPU Pricing Scraper
Extracts A100 pricing from civo.com using Selenium for dynamic content
NO FALLBACK VALUES - only live data from civo.com
"""

import json
import time
import re
from typing import Dict, Optional
from pathlib import Path

# Try Selenium first (for dynamic content)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# Fallback to requests for static content
import requests
from bs4 import BeautifulSoup


class CivoA100Scraper:
    """Scraper for Civo A100 pricing - uses Selenium for dynamic content"""

    def __init__(self):
        self.name = "Civo"
        self.base_url = "https://www.civo.com/pricing"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }

    def _get_selenium_driver(self):
        """Initialize Selenium WebDriver"""
        if not SELENIUM_AVAILABLE:
            return None
            
        try:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument(f'user-agent={self.headers["User-Agent"]}')
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            return driver
        except Exception as e:
            print(f"  Selenium init error: {e}")
            return None

    def _scrape_with_selenium(self) -> Dict[str, str]:
        """Scrape Civo pricing page using Selenium"""
        a100_prices = {}
        driver = self._get_selenium_driver()
        
        if not driver:
            print("  Selenium not available, trying requests...")
            return self._scrape_with_requests()
        
        try:
            print(f"  Loading {self.base_url} with Selenium...")
            driver.get(self.base_url)
            
            # Wait for page to load
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)  # Additional wait for JS content
            
            # Scroll to GPU pricing section
            try:
                gpu_section = driver.find_element(By.ID, "nvidia-gpus")
                driver.execute_script("arguments[0].scrollIntoView();", gpu_section)
                time.sleep(2)
            except:
                # Scroll down to load dynamic content
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                time.sleep(2)
            
            page_source = driver.page_source
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(page_source, 'html.parser')
            text_content = soup.get_text(separator=' ')
            
            # Look for A100 pricing patterns
            # Civo format: "NVIDIA A100 40GB" or "NVIDIA A100 80GB" with prices
            
            # Pattern 1: Look for A100 40GB and 80GB sections
            a100_40gb_patterns = [
                r'A100\s*40GB.*?\$(\d+\.?\d*)\s*(?:per\s*hour|/hr|/hour)',
                r'NVIDIA\s*A100\s*40GB.*?\$(\d+\.?\d*)',
                r'A100\s*40.*?\$(\d+\.?\d*)',
            ]
            
            a100_80gb_patterns = [
                r'A100\s*80GB.*?\$(\d+\.?\d*)\s*(?:per\s*hour|/hr|/hour)',
                r'NVIDIA\s*A100\s*80GB.*?\$(\d+\.?\d*)',
                r'A100\s*80.*?\$(\d+\.?\d*)',
            ]
            
            # Search for 40GB prices
            for pattern in a100_40gb_patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE | re.DOTALL)
                if matches:
                    for price_str in matches:
                        try:
                            price = float(price_str)
                            if 0.5 < price < 20:  # Reasonable A100 price range
                                if 'A100 40GB' not in a100_prices:
                                    a100_prices['A100 40GB'] = f"${price:.2f}/hr"
                                    print(f"    Found A100 40GB: ${price:.2f}/hr")
                                break
                        except ValueError:
                            continue
                if 'A100 40GB' in a100_prices:
                    break
            
            # Search for 80GB prices
            for pattern in a100_80gb_patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE | re.DOTALL)
                if matches:
                    for price_str in matches:
                        try:
                            price = float(price_str)
                            if 0.5 < price < 25:  # 80GB typically more expensive
                                if 'A100 80GB' not in a100_prices:
                                    a100_prices['A100 80GB'] = f"${price:.2f}/hr"
                                    print(f"    Found A100 80GB: ${price:.2f}/hr")
                                break
                        except ValueError:
                            continue
                if 'A100 80GB' in a100_prices:
                    break
            
            # Generic A100 pattern if specific variants not found
            if not a100_prices:
                generic_patterns = [
                    r'A100.*?\$(\d+\.?\d*)\s*(?:per\s*hour|/hr|/hour)',
                    r'A100.*?\$(\d+\.?\d*)',
                ]
                for pattern in generic_patterns:
                    matches = re.findall(pattern, text_content, re.IGNORECASE | re.DOTALL)
                    for price_str in matches:
                        try:
                            price = float(price_str)
                            if 0.5 < price < 25:
                                a100_prices['A100'] = f"${price:.2f}/hr"
                                print(f"    Found A100: ${price:.2f}/hr")
                                break
                        except ValueError:
                            continue
                    if a100_prices:
                        break
            
        except Exception as e:
            print(f"  Selenium scraping error: {e}")
        finally:
            if driver:
                driver.quit()
        
        return a100_prices

    def _scrape_with_requests(self) -> Dict[str, str]:
        """Fallback: Scrape using requests (may not work for dynamic content)"""
        a100_prices = {}
        
        try:
            response = requests.get(self.base_url, headers=self.headers, timeout=20)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            text_content = soup.get_text()
            
            patterns = [
                (r'A100\s*40GB.*?\$(\d+\.?\d*)', 'A100 40GB'),
                (r'A100\s*80GB.*?\$(\d+\.?\d*)', 'A100 80GB'),
                (r'A100.*?\$(\d+\.?\d*)', 'A100'),
            ]
            
            for pattern, name in patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE | re.DOTALL)
                if matches and name not in a100_prices:
                    try:
                        price = float(matches[0])
                        if 0.5 < price < 25:
                            a100_prices[name] = f"${price:.2f}/hr"
                    except ValueError:
                        pass
            
        except Exception as e:
            print(f"  Requests error: {e}")
        
        return a100_prices

    def get_a100_prices(self) -> Dict[str, str]:
        """Main method to extract A100 prices"""
        print(f"Fetching {self.name} A100 pricing...")
        print(f"URL: {self.base_url}")
        print("=" * 60)

        # Try Selenium first for dynamic content
        a100_prices = self._scrape_with_selenium()

        if a100_prices:
            print(f"  Found {len(a100_prices)} A100 price variants:")
            for variant, price in a100_prices.items():
                print(f"    - {variant}: {price}")
        else:
            print("  No A100 prices found")

        return a100_prices


def main():
    """Main entry point"""
    scraper = CivoA100Scraper()
    prices = scraper.get_a100_prices()

    # Save results - NO fallback values
    output = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'provider': scraper.name,
        'gpu_model': 'A100',
        'fetch_status': 'success' if prices else 'failed',
        'prices': prices
    }

    output_file = 'civo_a100_prices.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
