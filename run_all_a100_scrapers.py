#!/usr/bin/env python3
"""
Run All A100 Scrapers
Executes all A100 GPU price scrapers and combines results into a single JSON file.

This script:
1. Runs each scraper in the a100 directory
2. Combines all individual JSON outputs into a100_combined_prices.json
3. Provides a summary of all extracted prices

Supported Non-Hyperscaler Providers:
- HyperStack, CUDO Compute, RunPod, Vast.ai, Genesis Cloud
- JarvisLabs, GPU-Mart, Hostkey, Civo, Atlantic.Net
- FluidStack, Nebius, Lambda Labs, Paperspace

Hyperscaler Providers:
- AWS, Azure, GCP, Oracle
"""

import subprocess
import sys
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import re


# List of all supported providers
NON_HYPERSCALER_PROVIDERS = [
    "HyperStack",
    "CUDO Compute",
    "RunPod",
    "Vast.ai",
    "Genesis Cloud",
    "JarvisLabs",
    "GPU-Mart",
    "Hostkey",
    "Civo",
    "Atlantic.Net",
    "FluidStack",
    "Nebius",
    "Lambda Labs",
    "Paperspace",
]

HYPERSCALER_PROVIDERS = [
    "AWS",
    "Azure",
    "GCP",
    "Oracle",
]


class A100ScraperRunner:
    """Runner for all A100 GPU scrapers"""

    def __init__(self, a100_dir: str = "."):
        self.a100_dir = Path(a100_dir)
        self.python_exe = sys.executable

    def find_all_scrapers(self) -> List[Path]:
        """Find all A100 scraper files"""
        scrapers = list(self.a100_dir.glob("*_a100_scraper.py"))
        return sorted(scrapers)
    
    def run_scraper(self, scraper_path: Path) -> bool:
        """Run a single scraper and return success status"""
        print(f"\n{'='*60}")
        print(f"🔄 Running: {scraper_path.name}")
        print('='*60)
        
        try:
            result = subprocess.run(
                [self.python_exe, str(scraper_path)],
                cwd=str(self.a100_dir),
                capture_output=True,
                text=True,
                timeout=120  # 2 minute timeout per scraper
            )
            
            if result.returncode == 0:
                print(f"✅ {scraper_path.name} completed successfully")
                return True
            else:
                print(f"❌ {scraper_path.name} failed with return code {result.returncode}")
                if result.stderr:
                    print(f"   Error: {result.stderr[:200]}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"⏰ {scraper_path.name} timed out after 120 seconds")
            return False
        except Exception as e:
            print(f"❌ Error running {scraper_path.name}: {str(e)[:100]}")
            return False
    
    def run_all_scrapers(self) -> Dict[str, bool]:
        """Run all scrapers and return results"""
        scrapers = self.find_all_scrapers()
        print(f"\n📋 Found {len(scrapers)} A100 scrapers\n")
        
        results = {}
        for scraper in scrapers:
            results[scraper.name] = self.run_scraper(scraper)
        
        return results
    
    def combine_prices(self) -> Dict:
        """Combine all A100 price JSON files into one"""
        json_files = list(self.a100_dir.glob("*_a100_prices.json"))

        combined = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "gpu_model": "A100",
            "total_providers": 0,
            "successful_fetches": 0,
            "failed_fetches": 0,
            "total_variants": 0,
            "providers": {},
            "all_prices": [],
            "price_summary": []
        }

        print(f"\n{'='*60}")
        print("📦 COMBINING ALL A100 PRICES")
        print('='*60)

        for json_file in sorted(json_files):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                provider_name = data.get("provider", json_file.stem.replace("_a100_prices", ""))

                # Extract all prices and variants
                prices, has_error = self._extract_all_prices(data)

                combined["total_providers"] += 1

                if prices and not has_error:
                    combined["providers"][provider_name] = {
                        "source_file": json_file.name,
                        "variants": prices,
                        "variant_count": len(prices),
                        "fetch_status": "success"
                    }

                    # Add to all_prices list for sorting
                    for variant, price_info in prices.items():
                        combined["all_prices"].append({
                            "provider": provider_name,
                            "variant": variant,
                            "price": price_info["price"],
                            "currency": price_info["currency"],
                            "price_display": price_info["display"]
                        })
                        combined["total_variants"] += 1

                    # Get lowest price for summary
                    lowest = min(prices.values(), key=lambda x: x["price"])
                    combined["price_summary"].append({
                        "provider": provider_name,
                        "price": lowest["price"],
                        "currency": lowest["currency"],
                        "variant_count": len(prices),
                        "status": "success"
                    })
                    combined["successful_fetches"] += 1

                    print(f"   ✓ {provider_name:20s} {len(prices)} variant(s)")
                    for variant, price_info in prices.items():
                        print(f"      - {variant}: {price_info['display']}")
                else:
                    combined["providers"][provider_name] = {
                        "source_file": json_file.name,
                        "variants": {},
                        "variant_count": 0,
                        "fetch_status": "failed",
                        "error": data.get("prices", {}).get("Error", "No A100 prices found")
                    }
                    combined["price_summary"].append({
                        "provider": provider_name,
                        "price": None,
                        "currency": None,
                        "variant_count": 0,
                        "status": "failed"
                    })
                    combined["failed_fetches"] += 1
                    print(f"   ✗ {provider_name:20s} No A100 prices found")

            except Exception as e:
                print(f"   ✗ Error loading {json_file.name}: {e}")

        # Sort all_prices by price
        combined["all_prices"] = sorted(
            combined["all_prices"],
            key=lambda x: x["price"]
        )

        # Sort price summary by price (successes first, then failures)
        combined["price_summary"] = sorted(
            combined["price_summary"],
            key=lambda x: (x["price"] is None, x["price"] or 999)
        )

        return combined

    def _extract_all_prices(self, data: Dict) -> Tuple[Dict, bool]:
        """Extract all prices from provider data, returns (prices_dict, has_error)"""
        prices = {}
        has_error = False

        # Check for error in prices
        if "prices" in data:
            if "Error" in data["prices"]:
                has_error = True
                return prices, has_error

            for variant, price_str in data["prices"].items():
                if variant == "Error":
                    has_error = True
                    continue

                # Parse price string (e.g., "$1.35/hr", "€0.47/hr")
                price_str = str(price_str)

                # Detect currency
                currency = "USD"
                if "€" in price_str:
                    currency = "EUR"
                elif "£" in price_str:
                    currency = "GBP"
                elif "₹" in price_str:
                    currency = "INR"

                # Extract numeric price
                match = re.search(r'([0-9.]+)', price_str)
                if match:
                    try:
                        price = float(match.group(1))
                        if price > 0:
                            prices[variant] = {
                                "price": price,
                                "currency": currency,
                                "display": price_str.strip()
                            }
                    except ValueError:
                        pass

        # Try nested providers structure (legacy format)
        if not prices and "providers" in data:
            for provider_name, provider_data in data["providers"].items():
                if "variants" in provider_data:
                    for variant_name, variant_data in provider_data["variants"].items():
                        if isinstance(variant_data, dict) and "price_per_hour" in variant_data:
                            price = variant_data["price_per_hour"]
                            if price is not None and price > 0:
                                currency = variant_data.get("currency", "USD")
                                prices[variant_name] = {
                                    "price": float(price),
                                    "currency": currency,
                                    "display": f"${price:.2f}/hr" if currency == "USD" else f"{price:.2f} {currency}/hr"
                                }

        return prices, has_error
    
    def save_combined(self, combined: Dict, filename: str = "a100_combined_prices.json"):
        """Save combined prices to JSON"""
        output_path = self.a100_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(combined, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Combined prices saved to: {output_path}")
        return output_path


def main():
    """Main function to run all A100 scrapers"""
    print("🚀 A100 GPU Price Scraper Runner")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nSupported Providers:")
    print(f"  Non-Hyperscalers: {', '.join(NON_HYPERSCALER_PROVIDERS)}")
    print(f"  Hyperscalers: {', '.join(HYPERSCALER_PROVIDERS)}")

    runner = A100ScraperRunner()

    # Run all scrapers
    print("\n📋 PHASE 1: Running All Scrapers")
    print("=" * 60)
    results = runner.run_all_scrapers()

    # Print summary
    print("\n" + "=" * 60)
    print("📊 SCRAPER EXECUTION SUMMARY")
    print("=" * 60)

    successful = sum(1 for v in results.values() if v)
    failed = len(results) - successful

    for scraper, success in results.items():
        status = "✅" if success else "❌"
        print(f"   {status} {scraper}")

    print(f"\n   Total: {len(results)} scrapers")
    print(f"   ✅ Successful: {successful}")
    print(f"   ❌ Failed: {failed}")

    # Combine all prices
    print("\n📋 PHASE 2: Combining All Prices")
    combined = runner.combine_prices()

    # Save combined file
    runner.save_combined(combined)

    # Print all prices sorted by price
    print("\n" + "=" * 60)
    print("🎯 ALL A100 PRICES (Sorted Lowest to Highest)")
    print("=" * 60)

    if combined["all_prices"]:
        for item in combined["all_prices"]:
            currency_symbol = "$" if item["currency"] == "USD" else "€" if item["currency"] == "EUR" else item["currency"]
            print(f"   {item['provider']:18s} {item['variant']:30s} {currency_symbol}{item['price']:.2f}/hr")

    # Print provider summary
    print("\n" + "=" * 60)
    print("📋 PROVIDER SUMMARY")
    print("=" * 60)

    for item in combined["price_summary"]:
        if item["price"] is not None:
            currency_symbol = "$" if item.get("currency") == "USD" else "€" if item.get("currency") == "EUR" else "$"
            print(f"   ✓ {item['provider']:20s} {item['variant_count']} variant(s), lowest: {currency_symbol}{item['price']:.2f}/hr")
        else:
            print(f"   ✗ {item['provider']:20s} No A100 prices found")

    print(f"\n✅ Total providers: {combined['total_providers']}")
    print(f"   ✅ With A100 data: {combined['successful_fetches']}")
    print(f"   ❌ Without data: {combined['failed_fetches']}")
    print(f"   📊 Total variants: {combined['total_variants']}")

    # Price statistics for successful fetches
    if combined["all_prices"]:
        usd_prices = [p for p in combined["all_prices"] if p["currency"] == "USD"]
        if usd_prices:
            min_price = min(usd_prices, key=lambda x: x["price"])
            max_price = max(usd_prices, key=lambda x: x["price"])
            avg_price = sum(p["price"] for p in usd_prices) / len(usd_prices)

            print(f"\n📊 Price Statistics (USD only):")
            print(f"   Lowest:  {min_price['provider']} - {min_price['variant']} at ${min_price['price']:.2f}/hr")
            print(f"   Highest: {max_price['provider']} - {max_price['variant']} at ${max_price['price']:.2f}/hr")
            print(f"   Average: ${avg_price:.2f}/hr")

    print(f"\n⏱️  Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
