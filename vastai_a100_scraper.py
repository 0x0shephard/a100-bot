#!/usr/bin/env python3
"""
Vast.ai A100 GPU Pricing Scraper
Extracts A100 pricing from Vast.ai API - Dynamic pricing only
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
from typing import Dict, Optional


class VastAIA100Scraper:
    """Scraper for Vast.ai A100 pricing"""

    def __init__(self):
        self.name = "Vast.ai"
        self.base_url = "https://vast.ai/pricing"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }

    def get_a100_prices(self) -> Dict[str, str]:
        """Main method to extract A100 prices"""
        print(f"Fetching {self.name} A100 pricing...")
        print(f"URL: {self.base_url}")
        print("=" * 60)

        a100_prices = {}

        # Try Vast.ai API endpoints
        api_urls = [
            "https://vast.ai/api/v0/bundles",
            "https://cloud.vast.ai/api/v0/gpu_types",
        ]

        for api_url in api_urls:
            try:
                print(f"  Trying API: {api_url}")
                response = requests.get(api_url, headers=self.headers, timeout=20)

                if response.status_code == 200:
                    data = response.json()
                    found_prices = self._extract_from_json(data)
                    if found_prices:
                        a100_prices.update(found_prices)
                        break
                else:
                    print(f"    Status: {response.status_code}")

            except Exception as e:
                print(f"    API error: {str(e)[:50]}")
                continue

        # Fallback to page scraping if API fails
        if not a100_prices:
            a100_prices = self._scrape_pricing_page()

        if a100_prices:
            print(f"  Found {len(a100_prices)} A100 price variants:")
            for variant, price in a100_prices.items():
                print(f"    - {variant}: {price}")
        else:
            print("  No A100 prices found")

        return a100_prices

    def _extract_from_json(self, data, path="") -> Dict[str, str]:
        """Recursively search JSON for A100 pricing"""
        prices = {}

        if isinstance(data, dict):
            for key, value in data.items():
                key_str = str(key).lower()

                # Check if this entry is about A100
                if 'a100' in key_str:
                    if isinstance(value, (int, float)) and 0.1 < value < 20:
                        prices['A100'] = f"${value:.2f}/hr"
                        return prices

                # Check displayName or name fields
                if key in ['displayName', 'name', 'gpu_name', 'gpu_model']:
                    if isinstance(value, str) and 'A100' in value:
                        # Look for price in sibling keys
                        price_keys = ['price', 'dph', 'dph_total', 'min_bid', 'hourly_price']
                        for pk in price_keys:
                            if pk in data:
                                try:
                                    price = float(data[pk])
                                    if 0.1 < price < 20:
                                        prices[f'A100 ({value})'] = f"${price:.2f}/hr"
                                        return prices
                                except (ValueError, TypeError):
                                    pass

                # Recurse into nested structures
                if isinstance(value, (dict, list)):
                    nested = self._extract_from_json(value, f"{path}.{key}")
                    if nested:
                        prices.update(nested)
                        if prices:
                            return prices

        elif isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, (dict, list)):
                    nested = self._extract_from_json(item, f"{path}[{i}]")
                    if nested:
                        prices.update(nested)
                        if prices:
                            return prices

        return prices

    def _scrape_pricing_page(self) -> Dict[str, str]:
        """Scrape Vast.ai pricing page for A100"""
        a100_prices = {}

        try:
            print("  Trying page scraping...")
            response = requests.get(self.base_url, headers=self.headers, timeout=20)

            if response.status_code != 200:
                return a100_prices

            soup = BeautifulSoup(response.content, 'html.parser')
            text_content = soup.get_text()

            patterns = [
                (r'A100 PCIe.*?\$([0-9.]+)/hr', 'A100 PCIe'),
                (r'A100 SXM.*?\$([0-9.]+)/hr', 'A100 SXM'),
                (r'A100 80GB.*?\$([0-9.]+)/hr', 'A100 80GB'),
                (r'A100 40GB.*?\$([0-9.]+)/hr', 'A100 40GB'),
                (r'A100.*?\$([0-9.]+)/hr', 'A100'),
            ]

            for pattern, name in patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE)
                if matches and name not in a100_prices:
                    try:
                        price = float(matches[0])
                        if 0.1 < price < 20:
                            a100_prices[name] = f"${price:.2f}/hr"
                    except ValueError:
                        pass

        except Exception as e:
            print(f"    Scraping error: {str(e)[:50]}")

        return a100_prices


def main():
    """Main entry point"""
    scraper = VastAIA100Scraper()
    prices = scraper.get_a100_prices()

    # Save results
    output = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'provider': scraper.name,
        'gpu_model': 'A100',
        'prices': prices
    }

    output_file = 'vastai_a100_prices.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
