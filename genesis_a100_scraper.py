#!/usr/bin/env python3
"""
Genesis Cloud A100 GPU Pricing Scraper
Extracts A100 pricing from genesiscloud.com - Dynamic pricing only
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
from typing import Dict, Optional


class GenesisCloudA100Scraper:
    """Scraper for Genesis Cloud A100 pricing"""

    def __init__(self):
        self.name = "Genesis Cloud"
        self.base_url = "https://www.genesiscloud.com/pricing"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }

    def fetch_page(self) -> Optional[BeautifulSoup]:
        """Fetch the pricing page and return BeautifulSoup object"""
        try:
            response = requests.get(self.base_url, headers=self.headers, timeout=20)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            print(f"  Error fetching {self.name} page: {e}")
            return None

    def get_a100_prices(self) -> Dict[str, str]:
        """Main method to extract A100 prices"""
        print(f"Fetching {self.name} A100 pricing...")
        print(f"URL: {self.base_url}")
        print("=" * 60)

        soup = self.fetch_page()
        if not soup:
            return {'Error': f'Unable to fetch page from {self.name}'}

        a100_prices = {}
        text_content = soup.get_text()

        # Genesis Cloud patterns for A100
        patterns = [
            (r'A100 SXM.*?\$([0-9.]+)/hr', 'A100 SXM'),
            (r'A100 PCIe.*?\$([0-9.]+)/hr', 'A100 PCIe'),
            (r'A100 80GB.*?\$([0-9.]+)/hr', 'A100 80GB'),
            (r'A100 40GB.*?\$([0-9.]+)/hr', 'A100 40GB'),
            (r'A100 \(HGX\).*?\$([0-9.]+)/hr', 'A100 (HGX)'),
            (r'A100.*?\$([0-9.]+)/hr', 'A100'),
            # Alternative patterns
            (r'\$([0-9.]+)/hr.*?A100', 'A100'),
            (r'A100[^\$]{0,100}\$([0-9.]+)', 'A100'),
        ]

        for pattern, name in patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE | re.DOTALL)
            if matches and name not in a100_prices:
                try:
                    price = float(matches[0])
                    if 0.1 < price < 20:
                        a100_prices[name] = f"${price:.2f}/hr"
                except ValueError:
                    pass

        # Try to find in structured elements
        if not a100_prices:
            pricing_elements = soup.find_all(['div', 'tr', 'td', 'span'],
                                            class_=re.compile(r'price|pricing|gpu|card', re.I))
            for elem in pricing_elements:
                elem_text = elem.get_text()
                if 'A100' in elem_text:
                    price_matches = re.findall(r'\$([0-9.]+)', elem_text)
                    for price_str in price_matches:
                        try:
                            price = float(price_str)
                            if 0.1 < price < 20:
                                a100_prices['A100'] = f"${price:.2f}/hr"
                                break
                        except ValueError:
                            continue

        if a100_prices:
            print(f"  Found {len(a100_prices)} A100 price variants:")
            for variant, price in a100_prices.items():
                print(f"    - {variant}: {price}")
        else:
            print("  No A100 prices found")

        return a100_prices


def main():
    """Main entry point"""
    scraper = GenesisCloudA100Scraper()
    prices = scraper.get_a100_prices()

    # Save results
    output = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'provider': scraper.name,
        'gpu_model': 'A100',
        'prices': prices
    }

    output_file = 'genesis_a100_prices.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
