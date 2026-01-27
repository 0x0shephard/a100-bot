#!/usr/bin/env python3
"""
AWS P4d Instance (A100 GPU) Price Scraper
Extracts A100 pricing from AWS EC2 On-Demand pricing

AWS offers A100 GPUs in P4d instances (8 x A100 40GB GPUs).

Sources:
- Vantage.sh multi-region pricing
- AWS EC2 Pricing API / Website
- Selenium fallback

Reference: https://aws.amazon.com/ec2/pricing/on-demand/
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
from typing import Dict, Optional, List


class AWSA100Scraper:
    """Scraper for AWS P4d (A100) instance pricing"""
    
    def __init__(self):
        self.name = "AWS"
        self.base_url = "https://aws.amazon.com/ec2/pricing/on-demand/"
        self.vantage_url = "https://instances.vantage.sh/aws/ec2/p4d.24xlarge"
        # Multi-region URLs for volatility - P4d has A100 GPUs
        self.vantage_regions = [
            ("us-east-1", "https://instances.vantage.sh/aws/ec2/p4d.24xlarge?region=us-east-1"),
            ("us-east-2", "https://instances.vantage.sh/aws/ec2/p4d.24xlarge?region=us-east-2"),
            ("us-west-2", "https://instances.vantage.sh/aws/ec2/p4d.24xlarge?region=us-west-2"),
            ("eu-west-1", "https://instances.vantage.sh/aws/ec2/p4d.24xlarge?region=eu-west-1"),
            ("eu-central-1", "https://instances.vantage.sh/aws/ec2/p4d.24xlarge?region=eu-central-1"),
            ("ap-northeast-1", "https://instances.vantage.sh/aws/ec2/p4d.24xlarge?region=ap-northeast-1"),
        ]
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
    
    def get_a100_prices(self) -> Dict[str, str]:
        """Main method to extract A100 prices from AWS - BOTH Vantage AND direct sources"""
        print(f"🔍 Fetching {self.name} P4d (A100) pricing (multi-source)...")
        print("=" * 80)
        
        all_prices = {}
        sources_used = []
        
        # Try ALL methods and combine results for maximum data
        methods = [
            ("Vantage Multi-Region Pricing", self._try_vantage_multi_region),
            ("AWS EC2 Pricing Page (Selenium)", self._try_aws_pricing_selenium),
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
                    # A100 pricing should be reasonable (AWS is around $2.5-4/GPU/hr)
                    if 1.5 < price < 6:
                        return True
            except:
                continue
        return False
    
    def _try_vantage_multi_region(self) -> Dict[str, str]:
        """Fetch A100 prices from multiple AWS regions via Vantage.sh for volatility"""
        a100_prices = {}
        
        print(f"    Fetching prices from {len(self.vantage_regions)} AWS regions via Vantage...")
        
        for region_code, url in self.vantage_regions:
            try:
                response = requests.get(url, headers=self.headers, timeout=15)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    text_content = soup.get_text()
                    
                    # Look for pricing patterns
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
                                # Instance price for 8 A100 GPUs ~$22-35/hr
                                if 18 < price < 45:
                                    per_gpu_price = price / 8
                                    region_name = region_code.replace('-', ' ').title()
                                    variant_name = f"P4d.24xlarge Vantage ({region_name})"
                                    a100_prices[variant_name] = f"${per_gpu_price:.2f}/hr"
                                    print(f"      ✓ {region_code}: ${price:.2f}/instance → ${per_gpu_price:.2f}/GPU")
                                    break
                                # Already per-GPU price
                                elif 1.5 < price < 6:
                                    region_name = region_code.replace('-', ' ').title()
                                    variant_name = f"P4d.24xlarge Vantage ({region_name})"
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
    
    def _try_aws_pricing_selenium(self) -> Dict[str, str]:
        """Use Selenium to scrape pricing directly from AWS EC2 pricing page"""
        a100_prices = {}
        
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import WebDriverException, TimeoutException
            
            print("    Setting up Selenium WebDriver for AWS...")
            
            # Configure Chrome options
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                # Try AWS EC2 pricing page directly
                print(f"    Loading AWS EC2 pricing page...")
                driver.get(self.base_url)
                
                print("    Waiting for dynamic content to load...")
                time.sleep(8)  # Wait for JavaScript to render
                
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                text_content = soup.get_text()
                
                print(f"    ✓ Page loaded, content length: {len(text_content)}")
                
                # Check for P4d content
                if 'p4d' in text_content.lower() or 'P4d' in text_content:
                    print(f"      ✓ Found P4d content on AWS page")
                    
                    # Look for pricing patterns
                    price_patterns = [
                        r'p4d\.24xlarge[^$]*\$([0-9]+\.[0-9]+)',
                        r'\$([0-9]+\.[0-9]+)\s*(?:per\s+hour|/hr|hourly)',
                    ]
                    
                    for pattern in price_patterns:
                        matches = re.findall(pattern, text_content, re.IGNORECASE)
                        for match in matches:
                            try:
                                price = float(match)
                                if 18 < price < 45:
                                    per_gpu_price = price / 8
                                    print(f"      ✓ AWS Direct: ${price:.2f}/instance → ${per_gpu_price:.2f}/GPU")
                                    a100_prices['P4d.24xlarge AWS Direct (US East)'] = f"${per_gpu_price:.2f}/hr"
                                    break
                            except ValueError:
                                continue
                        if a100_prices:
                            break
                
                # Also try Vantage via Selenium as backup
                if not a100_prices:
                    print(f"    Loading Vantage pricing page via Selenium...")
                    driver.get(self.vantage_url)
                    time.sleep(5)
                    
                    page_source = driver.page_source
                    soup = BeautifulSoup(page_source, 'html.parser')
                    text_content = soup.get_text()
                    
                    print(f"    ✓ Vantage page loaded, content length: {len(text_content)}")
                    
                    price_patterns = [
                        r'\$([0-9]+\.[0-9]+)\s*(?:per\s+hour|/hr|hourly)',
                        r'On.?Demand[:\s]+\$([0-9]+\.[0-9]+)',
                        r'Linux[^$]*\$([0-9]+\.[0-9]+)',
                    ]
                    
                    for pattern in price_patterns:
                        matches = re.findall(pattern, text_content, re.IGNORECASE)
                        for match in matches:
                            try:
                                price = float(match)
                                if 18 < price < 45:
                                    per_gpu_price = price / 8
                                    print(f"      ✓ Vantage Selenium: ${price:.2f}/instance → ${per_gpu_price:.2f}/GPU")
                                    a100_prices['P4d.24xlarge Selenium (US East)'] = f"${per_gpu_price:.2f}/hr"
                                    return a100_prices
                            except ValueError:
                                continue
                
            finally:
                driver.quit()
                print("    WebDriver closed")
                
        except ImportError:
            print("      ⚠️  Selenium not installed. Run: pip install selenium")
        except Exception as e:
            print(f"      ⚠️  Error: {str(e)[:100]}")
        
        return a100_prices
    
    def _normalize_prices(self, prices: Dict[str, str]) -> Dict[str, str]:
        """
        Normalize prices - Calculate per-GPU pricing if needed.
        Calculate average across all sources and regions for a single representative price.
        """
        if not prices:
            return {}
        
        per_gpu_prices = []
        
        print("\n   📊 Normalizing AWS P4d (A100) pricing...")
        
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
                'P4d.24xlarge (AWS)': f"${avg_per_gpu:.2f}/hr"
            }
        
        return {}
    
    def save_to_json(self, prices: Dict[str, str], filename: str = "aws_a100_prices.json") -> bool:
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
                "data_sources": ["Vantage.sh", "AWS EC2 Pricing Page"],
                "providers": {
                    "AWS": {
                        "name": "AWS",
                        "url": self.base_url,
                        "variants": {
                            "P4d.24xlarge (AWS)": {
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
                    "instance_type": "P4d.24xlarge",
                    "gpu_model": "NVIDIA A100",
                    "gpu_memory": "40GB HBM2e",
                    "gpu_count_per_instance": 8,
                    "pricing_type": "On-Demand",
                    "source": "https://aws.amazon.com/ec2/pricing/on-demand/"
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
    """Main function to run the AWS P4d (A100) scraper"""
    print("🚀 AWS P4d (A100) GPU Pricing Scraper")
    print("=" * 80)
    print("Note: AWS offers A100 GPUs in P4d.24xlarge instances (8 x A100 40GB)")
    print("Sources: Vantage.sh + AWS EC2 Pricing Page")
    print("=" * 80)
    
    scraper = AWSA100Scraper()
    
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
