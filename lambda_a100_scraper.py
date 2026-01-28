#!/usr/bin/env python3
"""
Lambda Labs A100 GPU Pricing Scraper
Extracts A100 pricing from lambdalabs.com - Dynamic pricing only
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
from typing import Dict, Optional


class LambdaLabsA100Scraper:
    """Scraper for Lambda Labs A100 pricing"""

    def __init__(self):
        self.name = "Lambda Labs"
        self.base_url = "https://lambdalabs.com/service/gpu-cloud"
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

        a100_prices = {}

        # Try Lambda Labs API first
        api_prices = self._try_api()
        if api_prices:
            a100_prices.update(api_prices)

        # Try page scraping if API fails
        if not a100_prices:
            soup = self.fetch_page()
            if soup:
                a100_prices = self._scrape_page(soup)

        if a100_prices:
            print(f"  Found {len(a100_prices)} A100 price variants:")
            for variant, price in a100_prices.items():
                print(f"    - {variant}: {price}")
        else:
            print("  No A100 prices found")

        return a100_prices

    def _try_api(self) -> Dict[str, str]:
        """Try Lambda Labs API for A100 pricing"""
        a100_prices = {}

        try:
            print("  Trying Lambda Labs API...")
            api_response = requests.get(
                "https://cloud.lambdalabs.com/api/v1/instance-types",
                headers=self.headers,
                timeout=15
            )

            if api_response.status_code == 200:
                data = api_response.json()
                print(f"    Got API response")

                if 'data' in data:
                    for instance_type, details in data['data'].items():
                        instance_str = str(instance_type).upper()
                        details_str = str(details).upper()

                        if 'A100' in instance_str or 'A100' in details_str:
                            print(f"    Found A100 instance: {instance_type}")

                            if isinstance(details, dict) and 'instance_type' in details:
                                instance_info = details['instance_type']
                                if 'price_cents_per_hour' in instance_info:
                                    price = instance_info['price_cents_per_hour'] / 100
                                    if 0.1 < price < 20:
                                        a100_prices[f'A100 ({instance_type})'] = f"${price:.2f}/hr"
                            elif isinstance(details, dict) and 'price_cents_per_hour' in details:
                                price = details['price_cents_per_hour'] / 100
                                if 0.1 < price < 20:
                                    a100_prices[f'A100 ({instance_type})'] = f"${price:.2f}/hr"
            else:
                print(f"    API returned status: {api_response.status_code}")

        except Exception as e:
            print(f"    API error: {str(e)[:50]}")

        return a100_prices

    def _scrape_page(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Scrape Lambda Labs page for A100 pricing"""
        a100_prices = {}
        text_content = soup.get_text()

        patterns = [
            (r'1x A100.*?\$(\d+\.\d+)/hr', 'A100 (1x GPU)'),
            (r'2x A100.*?\$(\d+\.\d+)/hr', 'A100 (2x GPUs)'),
            (r'4x A100.*?\$(\d+\.\d+)/hr', 'A100 (4x GPUs)'),
            (r'8x A100.*?\$(\d+\.\d+)/hr', 'A100 (8x GPUs)'),
            (r'A100 SXM.*?\$(\d+\.\d+)', 'A100 SXM'),
            (r'A100 PCIe.*?\$(\d+\.\d+)', 'A100 PCIe'),
            (r'A100 80GB.*?\$(\d+\.\d+)', 'A100 80GB'),
            (r'A100 40GB.*?\$(\d+\.\d+)', 'A100 40GB'),
            (r'A100.*?\$(\d+\.\d+)/hr', 'A100'),
            (r'A100.*?\$(\d+\.\d+)', 'A100'),
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

        return a100_prices


def main():
    """Main entry point"""
    scraper = LambdaLabsA100Scraper()
    prices = scraper.get_a100_prices()

    # Save results
    output = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'provider': scraper.name,
        'gpu_model': 'A100',
        'prices': prices
    }

    output_file = 'lambda_a100_prices.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
