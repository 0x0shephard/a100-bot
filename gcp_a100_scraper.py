#!/usr/bin/env python3
"""
Google Cloud A2 (A100 GPU) Price Scraper
Extracts A100 pricing from Google Cloud Compute Engine pricing

GCP offers A100 GPUs in A2 instances:
- A2-highgpu: 8 x A100 40GB GPUs
- A2-ultragpu: 8 x A100 80GB GPUs

Sources:
- Vantage.sh multi-region pricing
- GCP Compute GPU Pricing Page (direct)
- Selenium fallback

Reference: https://cloud.google.com/compute/gpus-pricing
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
from typing import Dict, Optional


class GCPA100Scraper:
    """Scraper for Google Cloud A2 (A100) instance pricing"""
    
    def __init__(self):
        self.name = "GCP"
        self.base_url = "https://cloud.google.com/compute/gpus-pricing"
        self.machine_types_url = "https://cloud.google.com/compute/docs/gpus"
        # Vantage.sh aggregates cloud pricing - A2-highgpu has A100 40GB GPUs
        self.vantage_url = "https://instances.vantage.sh/gcp/a2-highgpu-8g"
        # Multi-region URLs for volatility
        self.vantage_regions = [
            ("us-central1", "https://instances.vantage.sh/gcp/a2-highgpu-8g?region=us-central1"),
            ("us-east4", "https://instances.vantage.sh/gcp/a2-highgpu-8g?region=us-east4"),
            ("us-west1", "https://instances.vantage.sh/gcp/a2-highgpu-8g?region=us-west1"),
            ("europe-west4", "https://instances.vantage.sh/gcp/a2-highgpu-8g?region=europe-west4"),
            ("asia-east1", "https://instances.vantage.sh/gcp/a2-highgpu-8g?region=asia-east1"),
        ]
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
    
    def get_a100_prices(self) -> Dict[str, str]:
        """Main method to extract A100 prices - BOTH Vantage AND direct GCP sources"""
        print(f"🔍 Fetching {self.name} A2 (A100) pricing (multi-source)...")
        print("=" * 80)
        
        all_prices = {}
        sources_used = []
        
        # Try ALL methods and combine results for maximum data
        methods = [
            ("Vantage Multi-Region Pricing", self._try_vantage_multi_region),
            ("GCP GPU Pricing Page (Selenium)", self._try_gcp_pricing_selenium),
        ]
        
        for method_name, method_func in methods:
            print(f"\n📋 Method: {method_name}")
            try:
                prices = method_func()
                if prices and self._validate_prices(prices):
                    all_prices.update(prices)
                    sources_used.append(method_name)
                    print(f"   ✅ Found {len(prices)} A100 prices!")
                else:
                    print(f"   ❌ No valid prices found")
            except Exception as e:
                print(f"   ⚠️  Error: {str(e)[:100]}")
                continue
        
        if not all_prices:
            print("\n❌ All live methods failed - price wasn't fetched (NO FALLBACK)")
            return {"error": "Price not fetched - all live methods failed"}
        
        print(f"\n📊 Sources used: {', '.join(sources_used)}")
        
        # Normalize to per-GPU pricing
        normalized_prices = self._normalize_prices(all_prices)
        
        print(f"\n✅ Final extraction: {len(normalized_prices)} A100 price variants")
        return normalized_prices
    
    def _validate_prices(self, prices: Dict[str, str]) -> bool:
        """Validate that prices are in a reasonable range for A100 GPUs"""
        if not prices:
            return False
        
        for variant, price_str in prices.items():
            if 'Error' in variant or 'error' in variant:
                continue
            try:
                price_match = re.search(r'\$?([0-9.]+)', str(price_str))
                if price_match:
                    price = float(price_match.group(1))
                    # A100 pricing should be reasonable (GCP is around $3-7/GPU/hr)
                    if 2 < price < 10:
                        return True
            except:
                continue
        return False
    
    def _try_vantage_multi_region(self) -> Dict[str, str]:
        """Fetch A100 prices from multiple GCP regions via Vantage.sh"""
        a100_prices = {}
        
        print(f"    Fetching prices from {len(self.vantage_regions)} GCP regions via Vantage...")
        
        for region_code, url in self.vantage_regions:
            try:
                response = requests.get(url, headers=self.headers, timeout=15)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    text_content = soup.get_text()
                    
                    price_patterns = [
                        r'\$([0-9]+\.?[0-9]*)\s*(?:per\s+hour|/hr|/hour)',
                        r'On.?Demand[:\s]+\$([0-9]+\.?[0-9]*)',
                        r'hourly[:\s]+\$([0-9]+\.?[0-9]*)',
                        r'\$([0-9]+\.[0-9]+)',
                    ]
                    
                    for pattern in price_patterns:
                        matches = re.findall(pattern, text_content, re.IGNORECASE)
                        for match in matches:
                            try:
                                price = float(match)
                                # Instance price for 8 A100 GPUs ~$32-50/hr
                                if 25 < price < 60:
                                    per_gpu_price = price / 8
                                    region_name = region_code.replace('-', ' ').title()
                                    variant_name = f"A2-highgpu-8g Vantage ({region_name})"
                                    a100_prices[variant_name] = f"${per_gpu_price:.2f}/hr"
                                    print(f"      ✓ {region_code}: ${price:.2f}/instance → ${per_gpu_price:.2f}/GPU")
                                    break
                                elif 2 < price < 8:
                                    region_name = region_code.replace('-', ' ').title()
                                    variant_name = f"A2-highgpu-8g Vantage ({region_name})"
                                    a100_prices[variant_name] = f"${price:.2f}/hr"
                                    print(f"      ✓ {region_code}: ${price:.2f}/GPU")
                                    break
                            except ValueError:
                                continue
                        if region_code.replace('-', ' ').title() in str(a100_prices):
                            break
                            
            except Exception as e:
                print(f"      ⚠️ {region_code}: Error - {str(e)[:30]}")
                continue
        
        if a100_prices:
            print(f"    Found prices from {len(a100_prices)} regions via Vantage")
        
        return a100_prices
    
    def _try_gcp_pricing_selenium(self) -> Dict[str, str]:
        """Use Selenium to scrape pricing directly from GCP GPU pricing page"""
        a100_prices = {}
        
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            print("    Setting up Selenium WebDriver for GCP...")
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                # Try GCP GPU pricing page directly
                print(f"    Loading GCP GPU pricing page...")
                driver.get(self.base_url)
                
                print("    Waiting for dynamic content to load...")
                time.sleep(8)
                
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                text_content = soup.get_text()
                
                print(f"    ✓ Page loaded, content length: {len(text_content)}")
                
                # Check for A100 content
                if 'A100' in text_content or 'a100' in text_content.lower():
                    print(f"      ✓ Found A100 content on GCP page")
                    
                    # Extract from tables first
                    tables = soup.find_all('table')
                    for table in tables:
                        table_text = table.get_text()
                        if 'A100' in table_text or 'a2-highgpu' in table_text.lower():
                            rows = table.find_all('tr')
                            for row in rows:
                                row_text = row.get_text()
                                if ('A100' in row_text or 'a2-highgpu' in row_text.lower()) and '$' in row_text:
                                    price_matches = re.findall(r'\$([0-9.]+)', row_text)
                                    for price_str in price_matches:
                                        try:
                                            price = float(price_str)
                                            if 2 < price < 8:
                                                a100_prices['A2-highgpu GCP Direct'] = f"${price:.2f}/hr"
                                                print(f"      ✓ GCP Direct: ${price:.2f}/GPU")
                                                break
                                            elif 25 < price < 60:
                                                per_gpu = price / 8
                                                a100_prices['A2-highgpu GCP Direct'] = f"${per_gpu:.2f}/hr"
                                                print(f"      ✓ GCP Direct: ${price:.2f}/instance → ${per_gpu:.2f}/GPU")
                                                break
                                        except ValueError:
                                            continue
                                    if a100_prices:
                                        break
                            if a100_prices:
                                break
                    
                    # Text-based extraction as fallback
                    if not a100_prices:
                        price_patterns = [
                            r'A100.*?\$([0-9.]+)',
                            r'a2-highgpu.*?\$([0-9.]+)',
                            r'\$([0-9]+\.[0-9]+).*?A100',
                        ]
                        
                        for pattern in price_patterns:
                            matches = re.findall(pattern, text_content, re.IGNORECASE)
                            for match in matches:
                                try:
                                    price = float(match)
                                    if 2 < price < 8:
                                        a100_prices['A2-highgpu GCP Direct'] = f"${price:.2f}/hr"
                                        print(f"      ✓ GCP Direct: ${price:.2f}/GPU")
                                        break
                                    elif 25 < price < 60:
                                        per_gpu = price / 8
                                        a100_prices['A2-highgpu GCP Direct'] = f"${per_gpu:.2f}/hr"
                                        print(f"      ✓ GCP Direct: ${price:.2f}/instance → ${per_gpu:.2f}/GPU")
                                        break
                                except ValueError:
                                    continue
                            if a100_prices:
                                break
                
            finally:
                driver.quit()
                print("    WebDriver closed")
                
        except ImportError:
            print("      ⚠️  Selenium not installed. Run: pip install selenium")
        except Exception as e:
            print(f"      ⚠️  Error: {str(e)[:100]}")
        
        return a100_prices
    
    def _normalize_prices(self, prices: Dict[str, str]) -> Dict[str, str]:
        """Normalize prices and average across all sources/regions"""
        if not prices:
            return {}
        
        per_gpu_prices = []
        
        print("\n   📊 Normalizing GCP A2 (A100) pricing...")
        
        for variant, price_str in prices.items():
            if 'Error' in variant or 'error' in variant:
                continue
            
            try:
                price_match = re.search(r'\$([0-9.]+)', price_str)
                if price_match:
                    price = float(price_match.group(1))
                    per_gpu_prices.append(price)
                    print(f"      {variant}: ${price:.2f}/hr")
                    
            except (ValueError, TypeError) as e:
                print(f"      ⚠️ Error normalizing {variant}: {e}")
                continue
        
        if per_gpu_prices:
            avg_per_gpu = sum(per_gpu_prices) / len(per_gpu_prices)
            min_price = min(per_gpu_prices)
            max_price = max(per_gpu_prices)
            
            print(f"\n   ✅ Averaged {len(per_gpu_prices)} prices → ${avg_per_gpu:.2f}/GPU")
            print(f"   📊 Price range: ${min_price:.2f} - ${max_price:.2f}/GPU (volatility: ${max_price - min_price:.2f})")
            
            return {
                'A2-highgpu-8g (GCP)': f"${avg_per_gpu:.2f}/hr"
            }
        
        return {}
    
    def save_to_json(self, prices: Dict[str, str], filename: str = "gcp_a100_prices.json") -> bool:
        """Save results to a JSON file"""
        try:
            has_error = "error" in prices or any("error" in str(v).lower() for v in prices.values())
            
            price_value = 0.0
            if not has_error:
                for variant, price_str in prices.items():
                    price_match = re.search(r'\$([0-9.]+)', price_str)
                    if price_match:
                        price_value = float(price_match.group(1))
                        break
            
            output_data = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "provider": self.name,
                "fetch_status": "failed" if has_error else "success",
                "data_sources": ["Vantage.sh", "GCP GPU Pricing Page"],
                "providers": {
                    "GCP": {
                        "name": "Google Cloud",
                        "url": self.base_url,
                        "variants": {
                            "A2-highgpu-8g (GCP)": {
                                "gpu_model": "A100",
                                "gpu_memory": "40GB",
                                "price_per_hour": price_value if not has_error else None,
                                "currency": "USD",
                                "availability": "on-demand",
                                "fetch_status": "failed" if has_error else "success"
                            }
                        }
                    }
                },
                "notes": {
                    "instance_type": "a2-highgpu-8g",
                    "gpu_model": "NVIDIA A100",
                    "gpu_memory": "40GB HBM2e",
                    "gpu_count_per_instance": 8,
                    "pricing_type": "On-Demand",
                    "source": "https://cloud.google.com/compute/gpus-pricing"
                }
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Results saved to: {filename}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving to file: {str(e)}")
            return False


def main():
    """Main function to run the GCP A2 (A100) scraper"""
    print("🚀 GCP A2 (A100) GPU Pricing Scraper")
    print("=" * 80)
    print("Note: GCP offers A100 GPUs in A2-highgpu-8g instances (8 x A100 40GB)")
    print("Sources: Vantage.sh + GCP GPU Pricing Page")
    print("=" * 80)
    
    scraper = GCPA100Scraper()
    
    start_time = time.time()
    prices = scraper.get_a100_prices()
    end_time = time.time()
    
    print(f"\n⏱️  Scraping completed in {end_time - start_time:.2f} seconds")
    
    if prices and 'error' not in str(prices).lower():
        print(f"\n✅ Successfully extracted {len(prices)} A100 price entries:\n")
        
        for variant, price in sorted(prices.items()):
            print(f"  • {variant:50s} {price}")
        
        scraper.save_to_json(prices)
    else:
        print("\n❌ No valid pricing data found - price wasn't fetched")
        scraper.save_to_json(prices)


if __name__ == "__main__":
    main()
