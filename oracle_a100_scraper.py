#!/usr/bin/env python3
"""
Oracle Cloud BM.GPU.A100 Price Scraper
Extracts A100 pricing from Oracle Cloud Infrastructure (OCI)

Oracle offers A100 GPUs in bare metal instances:
- BM.GPU4.8: 8 x A100 40GB GPUs
- BM.GPU.A100-v2.8: 8 x A100 80GB GPUs

Sources:
- Oracle Compute Pricing Page (direct)
- Vantage.sh multi-region
- Selenium fallback

Reference: https://www.oracle.com/cloud/compute/pricing/
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
from typing import Dict, Optional


class OracleA100Scraper:
    """Scraper for Oracle Cloud BM.GPU.A100 instance pricing"""
    
    def __init__(self):
        self.name = "Oracle"
        self.base_url = "https://www.oracle.com/cloud/compute/pricing/"
        self.gpu_url = "https://www.oracle.com/cloud/compute/gpu/"
        # Vantage.sh URLs for Oracle GPU instances
        self.vantage_url = "https://instances.vantage.sh/oracle/bm.gpu4.8"
        self.vantage_regions = [
            ("us-ashburn-1", "https://instances.vantage.sh/oracle/bm.gpu4.8?region=us-ashburn-1"),
            ("us-phoenix-1", "https://instances.vantage.sh/oracle/bm.gpu4.8?region=us-phoenix-1"),
            ("eu-frankfurt-1", "https://instances.vantage.sh/oracle/bm.gpu4.8?region=eu-frankfurt-1"),
            ("uk-london-1", "https://instances.vantage.sh/oracle/bm.gpu4.8?region=uk-london-1"),
            ("ap-tokyo-1", "https://instances.vantage.sh/oracle/bm.gpu4.8?region=ap-tokyo-1"),
        ]
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
    
    def get_a100_prices(self) -> Dict[str, str]:
        """Main method to extract A100 prices - BOTH Oracle direct AND Vantage"""
        print(f"🔍 Fetching {self.name} BM.GPU4.8 (A100) pricing (multi-source)...")
        print("=" * 80)
        
        all_prices = {}
        sources_used = []
        
        # Try ALL methods and combine results
        methods = [
            ("Vantage Multi-Region Pricing", self._try_vantage_multi_region),
            ("Oracle Pricing Page (Selenium)", self._try_oracle_pricing_selenium),
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
                    # Oracle A100 pricing is ~$3/GPU/hr
                    if 2 < price < 6:
                        return True
            except:
                continue
        return False
    
    def _try_vantage_multi_region(self) -> Dict[str, str]:
        """Fetch A100 prices from Vantage.sh for Oracle"""
        a100_prices = {}
        
        print(f"    Fetching prices from {len(self.vantage_regions)} Oracle regions via Vantage...")
        
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
                                # Instance price ~$24/hr for 8 GPUs
                                if 18 < price < 40:
                                    per_gpu_price = price / 8
                                    region_name = region_code.replace('-', ' ').title()
                                    variant_name = f"BM.GPU4.8 Vantage ({region_name})"
                                    a100_prices[variant_name] = f"${per_gpu_price:.2f}/hr"
                                    print(f"      ✓ Vantage {region_code}: ${price:.2f}/instance → ${per_gpu_price:.2f}/GPU")
                                    break
                                elif 2 < price < 6:
                                    region_name = region_code.replace('-', ' ').title()
                                    variant_name = f"BM.GPU4.8 Vantage ({region_name})"
                                    a100_prices[variant_name] = f"${price:.2f}/hr"
                                    print(f"      ✓ Vantage {region_code}: ${price:.2f}/GPU")
                                    break
                            except ValueError:
                                continue
                        if any(region_code.replace('-', ' ').title() in k for k in a100_prices.keys()):
                            break
                            
            except Exception as e:
                print(f"      ⚠️ {region_code}: Error - {str(e)[:30]}")
                continue
        
        if a100_prices:
            print(f"    Found {len(a100_prices)} prices via Vantage")
        
        return a100_prices
    
    def _try_oracle_pricing_selenium(self) -> Dict[str, str]:
        """Use Selenium to scrape Oracle pricing page directly"""
        a100_prices = {}
        
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            
            print("    Setting up Selenium WebDriver for Oracle...")
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                print(f"    Loading Oracle Compute pricing page...")
                driver.get(self.base_url)
                
                print("    Waiting for dynamic content to load...")
                time.sleep(8)
                
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                text_content = soup.get_text()
                
                print(f"    ✓ Page loaded, content length: {len(text_content)}")
                
                # Check for GPU/A100 content
                if 'GPU4' in text_content or 'A100' in text_content or 'BM.GPU' in text_content:
                    print(f"      ✓ Found GPU content on Oracle page")
                    
                    # Look for pricing in tables
                    tables = soup.find_all('table')
                    for table in tables:
                        table_text = table.get_text()
                        if 'GPU4' in table_text or 'A100' in table_text:
                            rows = table.find_all('tr')
                            for row in rows:
                                cells = row.find_all(['td', 'th'])
                                row_text = ' '.join([c.get_text().strip() for c in cells])
                                
                                if 'GPU4' in row_text or ('A100' in row_text and '40' in row_text):
                                    # Get last cell which contains price
                                    if len(cells) >= 2:
                                        last_cell = cells[-1].get_text().strip()
                                        price_match = re.search(r'\$([0-9.]+)', last_cell)
                                        if price_match:
                                            try:
                                                price = float(price_match.group(1))
                                                if 2 <= price <= 5:
                                                    a100_prices['BM.GPU4.8 Oracle Direct'] = f"${price:.2f}/hr"
                                                    print(f"      ✓ Oracle Direct: ${price:.2f}/GPU")
                                                    break
                                            except ValueError:
                                                continue
                                    
                                    # Fallback to any price in row
                                    price_matches = re.findall(r'\$([0-9.]+)', row_text)
                                    for price_str in price_matches:
                                        try:
                                            price = float(price_str)
                                            if 2 <= price <= 5:
                                                a100_prices['BM.GPU4.8 Oracle Direct'] = f"${price:.2f}/hr"
                                                print(f"      ✓ Oracle Direct: ${price:.2f}/GPU")
                                                break
                                        except ValueError:
                                            continue
                                    if a100_prices:
                                        break
                            if a100_prices:
                                break
                
            finally:
                driver.quit()
                print("    WebDriver closed")
                
        except ImportError:
            print("      ⚠️  Selenium not installed")
        except Exception as e:
            print(f"      ⚠️  Error: {str(e)[:100]}")
        
        return a100_prices
    
    def _normalize_prices(self, prices: Dict[str, str]) -> Dict[str, str]:
        """Normalize prices and average across all sources"""
        if not prices:
            return {}
        
        per_gpu_prices = []
        
        print("\n   📊 Normalizing Oracle BM.GPU4.8 (A100) pricing...")
        
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
                'BM.GPU4.8 (Oracle)': f"${avg_per_gpu:.2f}/hr"
            }
        
        return {}
    
    def save_to_json(self, prices: Dict[str, str], filename: str = "oracle_a100_prices.json") -> bool:
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
                "data_sources": ["Vantage.sh", "Oracle Compute Pricing Page"],
                "providers": {
                    "Oracle": {
                        "name": "Oracle Cloud",
                        "url": self.base_url,
                        "variants": {
                            "BM.GPU4.8 (Oracle)": {
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
                    "instance_type": "BM.GPU4.8",
                    "gpu_model": "NVIDIA A100",
                    "gpu_memory": "40GB HBM2e",
                    "gpu_count_per_instance": 8,
                    "pricing_type": "On-Demand (Pay-As-You-Go)",
                    "source": "https://www.oracle.com/cloud/compute/pricing/"
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
    """Main function to run the Oracle BM.GPU4.8 (A100) scraper"""
    print("🚀 Oracle Cloud BM.GPU4.8 (A100) GPU Pricing Scraper")
    print("=" * 80)
    print("Note: Oracle offers A100 GPUs in BM.GPU4.8 instances (8 x A100 40GB)")
    print("Sources: Vantage.sh + Oracle Compute Pricing Page")
    print("=" * 80)
    
    scraper = OracleA100Scraper()
    
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
