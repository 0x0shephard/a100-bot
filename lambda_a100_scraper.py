#!/usr/bin/env python3
"""
Lambda Labs A100 GPU Pricing Scraper
Extracts A100 pricing from Lambda Labs using the same approach as the H100 scraper
Based on: bot/bot/scraper20.py LambdaLabsScraper

NO FALLBACK VALUES - only live data
"""

import json
import time
import re
from typing import Dict, Optional
import requests
from bs4 import BeautifulSoup


class LambdaLabsA100Scraper:
    """Scraper for Lambda Labs A100 pricing"""

    def __init__(self):
        self.name = "Lambda Labs"
        self.base_url = "https://lambda.ai/pricing"
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

    def extract_a100_prices(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract A100 prices from Lambda Labs"""
        a100_prices = {}
        text_content = soup.get_text()
        
        # Lambda Labs A100 pricing patterns
        # Actual format observed: "TiB SSD$1.29NVIDIA A100"
        patterns = [
            # Pattern for text like "SSD$1.29NVIDIA A100" 
            (r'\$(\d+\.\d+)NVIDIA A100', 'A100'),
            (r'SSD\$(\d+\.\d+)NVIDIA A100', 'A100'),
            (r'\$(\d+\.\d+).*?NVIDIA A100', 'A100'),
            # Pattern: price followed by A100
            (r'\$(\d+\.\d+).*?A100', 'A100'),
            # Pattern: A100 somewhere near a price
            (r'A100.*?\$(\d+\.\d+)', 'A100'),
            # General price patterns near A100
            (r'(\d+\.\d+)/hr.*?A100', 'A100'),
            (r'A100.*?(\d+\.\d+)/GPU', 'A100 (per GPU)'),
            # On-demand patterns
            (r'On-demand.*?A100.*?\$(\d+\.\d+)', 'A100 (On-Demand)'),
            # Instance type patterns
            (r'gpu_1x_a100.*?\$(\d+\.\d+)', 'A100 (1x GPU)'),
        ]
        
        for pattern, name in patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE | re.DOTALL)
            if matches and name not in a100_prices:
                try:
                    price = float(matches[0])
                    if 0.5 < price < 20:  # Reasonable A100 price per GPU
                        a100_prices[name] = f"${matches[0]}"
                except ValueError:
                    a100_prices[name] = f"${matches[0]}"
        
        return a100_prices

    def get_a100_prices(self) -> Dict[str, str]:
        """Main method to extract A100 prices"""
        print(f"Fetching {self.name} A100 pricing...")
        print(f"URL: {self.base_url}")
        print("=" * 60)

        soup = self.fetch_page()
        if not soup:
            return {}

        a100_prices = self.extract_a100_prices(soup)

        if a100_prices:
            print(f"  Found {len(a100_prices)} A100 price variants:")
            for variant, price in a100_prices.items():
                print(f"    - {variant}: {price}")
        else:
            print("  No A100 prices found")

        return a100_prices


def main():
    """Main entry point"""
    scraper = LambdaLabsA100Scraper()
    prices = scraper.get_a100_prices()

    output = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'provider': scraper.name,
        'gpu_model': 'A100',
        'fetch_status': 'success' if prices else 'failed',
        'data_sources': ['Lambda Labs Pricing Page'],
        'prices': prices
    }

    output_file = 'lambda_a100_prices.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
