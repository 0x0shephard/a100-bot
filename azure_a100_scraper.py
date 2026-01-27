#!/usr/bin/env python3
"""
Azure ND A100 v4 Instance Price Scraper
Extracts A100 pricing from Microsoft Azure VM pricing

Azure offers A100 GPUs in ND A100 v4 series VMs:
- ND96asr_A100_v4: 8 x A100 40GB GPUs
- ND96amsr_A100_v4: 8 x A100 80GB GPUs

Sources:
- Azure Retail Prices API (primary - direct)
- Vantage.sh (secondary)
- Selenium fallback

Reference: https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
from typing import Dict, Optional


class AzureA100Scraper:
    """Scraper for Azure ND A100 v4 instance pricing"""
    
    def __init__(self):
        self.name = "Azure"
        self.base_url = "https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/"
        self.api_url = "https://prices.azure.com/api/retail/prices"
        self.vantage_url = "https://instances.vantage.sh/azure/nd96asr-a100-v4"
        self.vantage_regions = [
            ("eastus", "https://instances.vantage.sh/azure/nd96asr-a100-v4?region=eastus"),
            ("westus2", "https://instances.vantage.sh/azure/nd96asr-a100-v4?region=westus2"),
            ("centralus", "https://instances.vantage.sh/azure/nd96asr-a100-v4?region=centralus"),
            ("northeurope", "https://instances.vantage.sh/azure/nd96asr-a100-v4?region=northeurope"),
        ]
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
    
    def get_a100_prices(self) -> Dict[str, str]:
        """Main method to extract A100 prices - BOTH Azure API AND Vantage"""
        print(f"🔍 Fetching {self.name} ND A100 v4 pricing (multi-source)...")
        print("=" * 80)
        
        all_prices = {}
        sources_used = []
        
        # Try ALL methods and combine results
        methods = [
            ("Azure Retail Prices API (Direct)", self._try_azure_pricing_api),
            ("Vantage Multi-Region Pricing", self._try_vantage_multi_region),
            ("Azure Pricing Page (Selenium)", self._try_azure_pricing_selenium),
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
                    # A100 pricing should be reasonable (Azure is around $2.5-5/GPU/hr)
                    if 1.5 < price < 8:
                        return True
            except:
                continue
        return False
    
    def _try_azure_pricing_api(self) -> Dict[str, str]:
        """Use Azure Retail Prices API directly for multi-region pricing"""
        a100_prices = {}
        us_region_prices = []
        
        try:
            filter_queries = [
                "armSkuName eq 'Standard_ND96asr_A100_v4' and priceType eq 'Consumption'",
                "contains(armSkuName, 'ND96asr_A100') and priceType eq 'Consumption'",
                "contains(productName, 'ND A100 v4') and priceType eq 'Consumption'",
            ]
            
            print(f"    Trying Azure Retail Prices API (Direct)...")
            items = []
            
            for filter_query in filter_queries:
                api_url = f"{self.api_url}?$filter={filter_query}"
                print(f"    Filter: {filter_query[:60]}...")
                
                response = requests.get(api_url, headers=self.headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    items = data.get('Items', [])
                    
                    if items:
                        print(f"      ✓ API returned {len(items)} pricing items")
                        break
            
            if not items:
                print("      No items found")
                return a100_prices
            
            for item in items:
                sku_name = item.get('armSkuName', '')
                region = item.get('armRegionName', '')
                unit_price = item.get('unitPrice', 0)
                product_name = item.get('productName', '')
                
                # Filter for Linux VMs only
                if 'Windows' in product_name or 'Spot' in product_name or 'Low Priority' in product_name:
                    continue
                
                if ('A100' in sku_name or 'ND96asr' in sku_name) and unit_price > 0:
                    per_gpu_price = unit_price / 8
                    
                    region_display = region.replace('eastus', 'East US').replace('westus', 'West US')
                    region_display = region_display.replace('centralus', 'Central US')
                    
                    # Collect US region prices
                    if region and ('us' in region.lower() or 'central' in region.lower()):
                        us_region_prices.append({
                            'price': per_gpu_price,
                            'region': region,
                            'region_display': region_display,
                        })
                        print(f"        ✓ API {region}: ${unit_price:.2f}/instance → ${per_gpu_price:.2f}/GPU")
                    else:
                        variant_name = f"ND96asr_A100_v4 API ({region_display})"
                        a100_prices[variant_name] = f"${per_gpu_price:.2f}/hr"
            
            # Average US region prices
            if us_region_prices:
                avg_price = sum(p['price'] for p in us_region_prices) / len(us_region_prices)
                a100_prices['ND96asr_A100_v4 API (US Avg)'] = f"${avg_price:.2f}/hr"
                print(f"\n      ✅ Averaged {len(us_region_prices)} US API prices: ${avg_price:.2f}/GPU")
                
                for p in us_region_prices[:3]:
                    variant_name = f"ND96asr_A100_v4 API ({p['region_display']})"
                    a100_prices[variant_name] = f"${p['price']:.2f}/hr"
                    
        except Exception as e:
            print(f"      Error: {str(e)[:80]}...")
        
        return a100_prices
    
    def _try_vantage_multi_region(self) -> Dict[str, str]:
        """Fetch A100 prices from Vantage.sh for Azure"""
        a100_prices = {}
        
        print(f"    Fetching prices from {len(self.vantage_regions)} Azure regions via Vantage...")
        
        for region_code, url in self.vantage_regions:
            try:
                response = requests.get(url, headers=self.headers, timeout=15)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    text_content = soup.get_text()
                    
                    price_patterns = [
                        r'\$([0-9]+\.?[0-9]*)\s*(?:per\s+hour|/hr|/hour)',
                        r'On.?Demand[:\s]+\$([0-9]+\.?[0-9]*)',
                        r'Pay as you go[:\s]+\$([0-9]+\.?[0-9]*)',
                        r'\$([0-9]+\.[0-9]+)',
                    ]
                    
                    for pattern in price_patterns:
                        matches = re.findall(pattern, text_content, re.IGNORECASE)
                        for match in matches:
                            try:
                                price = float(match)
                                # Instance price ~$20-35/hr
                                if 18 < price < 45:
                                    per_gpu_price = price / 8
                                    region_name = region_code.replace('eastus', 'East US').replace('westus2', 'West US 2')
                                    variant_name = f"ND96asr_A100_v4 Vantage ({region_name})"
                                    a100_prices[variant_name] = f"${per_gpu_price:.2f}/hr"
                                    print(f"      ✓ Vantage {region_code}: ${price:.2f}/instance → ${per_gpu_price:.2f}/GPU")
                                    break
                                elif 1.5 < price < 6:
                                    region_name = region_code.replace('eastus', 'East US').replace('westus2', 'West US 2')
                                    variant_name = f"ND96asr_A100_v4 Vantage ({region_name})"
                                    a100_prices[variant_name] = f"${price:.2f}/hr"
                                    print(f"      ✓ Vantage {region_code}: ${price:.2f}/GPU")
                                    break
                            except ValueError:
                                continue
                        if any(region_code in k for k in a100_prices.keys()):
                            break
                            
            except Exception as e:
                print(f"      ⚠️ {region_code}: Error - {str(e)[:30]}")
                continue
        
        if a100_prices:
            print(f"    Found {len(a100_prices)} prices via Vantage")
        
        return a100_prices
    
    def _try_azure_pricing_selenium(self) -> Dict[str, str]:
        """Use Selenium to scrape Azure pricing page directly"""
        a100_prices = {}
        
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            
            print("    Setting up Selenium WebDriver for Azure...")
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                print(f"    Loading Azure VM pricing page...")
                driver.get(self.base_url)
                
                print("    Waiting for dynamic content to load...")
                time.sleep(8)
                
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                text_content = soup.get_text()
                
                print(f"    ✓ Page loaded, content length: {len(text_content)}")
                
                if 'ND96' in text_content or 'A100' in text_content:
                    print(f"      ✓ Found A100/ND96 content on Azure page")
                    
                    # Look for pricing in tables
                    tables = soup.find_all('table')
                    for table in tables:
                        table_text = table.get_text()
                        if 'ND96' in table_text or 'A100' in table_text:
                            rows = table.find_all('tr')
                            for row in rows:
                                row_text = row.get_text()
                                if 'ND96' in row_text and '$' in row_text:
                                    price_matches = re.findall(r'\$([0-9.]+)', row_text)
                                    for price_str in price_matches:
                                        try:
                                            price = float(price_str)
                                            if 18 < price < 45:
                                                per_gpu = price / 8
                                                a100_prices['ND96asr_A100_v4 Azure Direct'] = f"${per_gpu:.2f}/hr"
                                                print(f"      ✓ Azure Direct: ${price:.2f}/instance → ${per_gpu:.2f}/GPU")
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
        
        print("\n   📊 Normalizing Azure ND A100 v4 pricing...")
        
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
                'ND96asr_A100_v4 (Azure)': f"${avg_per_gpu:.2f}/hr"
            }
        
        return {}
    
    def save_to_json(self, prices: Dict[str, str], filename: str = "azure_a100_prices.json") -> bool:
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
                "data_sources": ["Azure Retail Prices API", "Vantage.sh", "Azure Pricing Page"],
                "providers": {
                    "Azure": {
                        "name": "Microsoft Azure",
                        "url": self.base_url,
                        "variants": {
                            "ND96asr_A100_v4 (Azure)": {
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
                    "instance_type": "Standard_ND96asr_A100_v4",
                    "gpu_model": "NVIDIA A100",
                    "gpu_memory": "40GB HBM2e",
                    "gpu_count_per_instance": 8,
                    "pricing_type": "On-Demand (Linux)",
                    "source": "https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/"
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
    """Main function to run the Azure ND A100 v4 scraper"""
    print("🚀 Azure ND A100 v4 GPU Pricing Scraper")
    print("=" * 80)
    print("Note: Azure offers A100 GPUs in ND96asr_A100_v4 instances (8 x A100 40GB)")
    print("Sources: Azure Retail API + Vantage.sh + Azure Pricing Page")
    print("=" * 80)
    
    scraper = AzureA100Scraper()
    
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
