#!/usr/bin/env python3
"""
RunPod A100 GPU Pricing Scraper
Extracts A100 pricing from RunPod via GraphQL API - Dynamic pricing only
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
from typing import Dict, Optional


class RunPodA100Scraper:
    """Scraper for RunPod A100 pricing"""

    def __init__(self):
        self.name = "RunPod"
        self.base_url = "https://www.runpod.io"
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

        # Try GraphQL API first
        a100_prices = self._try_graphql_api()

        if not a100_prices:
            # Fallback to page scraping
            a100_prices = self._scrape_pricing_page()

        if a100_prices:
            print(f"  Found {len(a100_prices)} A100 price variants:")
            for variant, price in a100_prices.items():
                print(f"    - {variant}: {price}")
        else:
            print("  No A100 prices found")

        return a100_prices

    def _try_graphql_api(self) -> Dict[str, str]:
        """Try RunPod GraphQL API for A100 pricing"""
        a100_prices = {}

        graphql_query = {
            "query": """
                query GpuTypes {
                    gpuTypes {
                        id
                        displayName
                        memoryInGb
                        secureCloud
                        communityCloud
                        lowestPrice {
                            minimumBidPrice
                            uninterruptablePrice
                        }
                    }
                }
            """
        }

        try:
            print("  Trying RunPod GraphQL API...")
            response = requests.post(
                "https://api.runpod.io/graphql",
                json=graphql_query,
                headers={**self.headers, 'Content-Type': 'application/json'},
                timeout=20
            )

            if response.status_code == 200:
                data = response.json()

                if 'data' in data and 'gpuTypes' in data['data']:
                    gpu_types = data['data']['gpuTypes']
                    print(f"    Found {len(gpu_types)} GPU types in API response")

                    for gpu in gpu_types:
                        if not isinstance(gpu, dict):
                            continue

                        display_name = gpu.get('displayName', '')

                        # Check for A100
                        if 'A100' in display_name or 'a100' in display_name.lower():
                            print(f"    Found A100 variant: {display_name}")
                            lowest_price = gpu.get('lowestPrice')

                            if lowest_price and isinstance(lowest_price, dict):
                                # Secure cloud price
                                uninterruptable = lowest_price.get('uninterruptablePrice')
                                if uninterruptable is not None:
                                    try:
                                        price = float(uninterruptable)
                                        if 0.1 < price < 20:
                                            variant_name = f"A100 ({display_name} - Secure)"
                                            a100_prices[variant_name] = f"${price:.2f}/hr"
                                    except (ValueError, TypeError):
                                        pass

                                # Spot price
                                minimum_bid = lowest_price.get('minimumBidPrice')
                                if minimum_bid is not None:
                                    try:
                                        price = float(minimum_bid)
                                        if 0.1 < price < 20:
                                            variant_name = f"A100 ({display_name} - Spot)"
                                            a100_prices[variant_name] = f"${price:.2f}/hr"
                                    except (ValueError, TypeError):
                                        pass
                            else:
                                print(f"      No pricing data for {display_name}")

        except Exception as e:
            print(f"    GraphQL API error: {str(e)[:50]}")

        return a100_prices

    def _scrape_pricing_page(self) -> Dict[str, str]:
        """Scrape RunPod pricing page for A100"""
        a100_prices = {}

        pricing_urls = [
            "https://www.runpod.io/pricing",
            "https://www.runpod.io/console/gpu-cloud",
        ]

        for url in pricing_urls:
            try:
                print(f"  Trying page: {url}")
                response = requests.get(url, headers=self.headers, timeout=20)

                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')
                text_content = soup.get_text()

                if 'A100' not in text_content:
                    print("    No A100 references on this page")
                    continue

                patterns = [
                    (r'A100\s+PCIe[^\$]{0,200}\$([0-9.]+)/hr', 'A100 PCIe'),
                    (r'A100\s+SXM[^\$]{0,200}\$([0-9.]+)/hr', 'A100 SXM'),
                    (r'A100\s+80GB[^\$]{0,200}\$([0-9.]+)/hr', 'A100 80GB'),
                    (r'A100\s+40GB[^\$]{0,200}\$([0-9.]+)/hr', 'A100 40GB'),
                    (r'A100[^\$\n]{0,200}\$([0-9.]+)/hr', 'A100'),
                ]

                for pattern, variant in patterns:
                    matches = re.findall(pattern, text_content, re.IGNORECASE | re.DOTALL)
                    for match in matches:
                        try:
                            price = float(match)
                            if 0.1 <= price <= 20.0 and variant not in a100_prices:
                                a100_prices[variant] = f"${price:.2f}/hr"
                        except ValueError:
                            continue

                if a100_prices:
                    return a100_prices

            except Exception as e:
                print(f"    Error scraping {url}: {str(e)[:50]}")
                continue

        return a100_prices


def main():
    """Main entry point"""
    scraper = RunPodA100Scraper()
    prices = scraper.get_a100_prices()

    # Save results
    output = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'provider': scraper.name,
        'gpu_model': 'A100',
        'prices': prices
    }

    output_file = 'runpod_a100_prices.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
