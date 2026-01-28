#!/usr/bin/env python3
"""
Civo A100 GPU Pricing Scraper
Extracts A100 pricing from civo.com - Dynamic pricing only
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
from typing import Dict, Optional


class CivoA100Scraper:
    """Scraper for Civo A100 pricing"""

    def __init__(self):
        self.name = "Civo"
        self.base_url = "https://www.civo.com/pricing"
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

        # Civo shows clear GPU pricing in their tables
        patterns = [
            # A100 SXM pricing patterns
            (r'Small 1 x NVIDIA A100.*?\$(\d+\.\d+) per hour.*?\$(\d+\.\d+) per hour', 'A100 SXM (1x GPU)'),
            (r'Extra Large 8 x NVIDIA A100.*?\$(\d+\.\d+) per hour.*?\$(\d+\.\d+) per hour', 'A100 SXM (8x GPUs)'),
            # A100 PCI pricing
            (r'1 x NVIDIA A100.*?PCI.*?\$(\d+\.\d+) per hour', 'A100 PCI (1x GPU)'),
            (r'2 x NVIDIA A100.*?PCI.*?\$(\d+\.\d+) per hour', 'A100 PCI (2x GPUs)'),
            (r'4 x NVIDIA A100.*?PCI.*?\$(\d+\.\d+) per hour', 'A100 PCI (4x GPUs)'),
            (r'8 x NVIDIA A100.*?PCI.*?\$(\d+\.\d+) per hour', 'A100 PCI (8x GPUs)'),
        ]

        for pattern, name in patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE | re.DOTALL)
            if matches:
                if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                    standard_price, discounted_price = matches[0]
                    a100_prices[f"{name} (Standard)"] = f"${standard_price}/hr"
                    a100_prices[f"{name} (Discounted)"] = f"${discounted_price}/hr"
                else:
                    a100_prices[name] = f"${matches[0]}/hr"

        # Fallback patterns
        if not a100_prices:
            fallback_patterns = [
                (r'1 x NVIDIA A100.*?\$(\d+\.\d+) per hour', 'A100 (1x GPU)'),
                (r'2 x NVIDIA A100.*?\$(\d+\.\d+) per hour', 'A100 (2x GPUs)'),
                (r'4 x NVIDIA A100.*?\$(\d+\.\d+) per hour', 'A100 (4x GPUs)'),
                (r'8 x NVIDIA A100.*?\$(\d+\.\d+) per hour', 'A100 (8x GPUs)'),
                (r'A100.*?\$(\d+\.\d+)', 'A100'),
            ]

            for pattern, name in fallback_patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE)
                if matches and name not in a100_prices:
                    a100_prices[name] = f"${matches[0]}/hr"

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

    # Save results
    output = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'provider': scraper.name,
        'gpu_model': 'A100',
        'prices': prices
    }

    output_file = 'civo_a100_prices.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
