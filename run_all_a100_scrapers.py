#!/usr/bin/env python3
"""
Run All A100 Scrapers
Executes all A100 GPU price scrapers and combines results into a single JSON file.

This script:
1. Runs each scraper in the a100 directory
2. Combines all individual JSON outputs into a100_combined_prices.json
3. Provides a summary of all extracted prices
"""

import subprocess
import sys
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import re


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
            "total_providers": 0,
            "successful_fetches": 0,
            "failed_fetches": 0,
            "providers": {},
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
                fetch_status = data.get("fetch_status", "unknown")
                
                # Extract price
                price = self._extract_price(data)
                
                combined["total_providers"] += 1
                
                if fetch_status == "success" and price and price > 0:
                    combined["providers"][provider_name] = {
                        "source_file": json_file.name,
                        "price_per_hour": round(price, 2),
                        "fetch_status": "success",
                        "data": data
                    }
                    combined["price_summary"].append({
                        "provider": provider_name,
                        "price": round(price, 2),
                        "status": "success"
                    })
                    combined["successful_fetches"] += 1
                    print(f"   ✓ {provider_name:25s} ${price:.2f}/hr")
                else:
                    combined["providers"][provider_name] = {
                        "source_file": json_file.name,
                        "price_per_hour": None,
                        "fetch_status": "failed",
                        "data": data
                    }
                    combined["price_summary"].append({
                        "provider": provider_name,
                        "price": None,
                        "status": "failed - price not fetched"
                    })
                    combined["failed_fetches"] += 1
                    print(f"   ✗ {provider_name:25s} FAILED - price not fetched")
                    
            except Exception as e:
                print(f"   ✗ Error loading {json_file.name}: {e}")
        
        # Sort price summary by price (successes first, then failures)
        combined["price_summary"] = sorted(
            combined["price_summary"], 
            key=lambda x: (x["price"] is None, x["price"] or 999)
        )
        
        return combined
    
    def _extract_price(self, data: Dict) -> float:
        """Extract price from provider data"""
        # Try nested providers structure
        if "providers" in data:
            for provider_name, provider_data in data["providers"].items():
                if "variants" in provider_data:
                    for variant_name, variant_data in provider_data["variants"].items():
                        if isinstance(variant_data, dict) and "price_per_hour" in variant_data:
                            price = variant_data["price_per_hour"]
                            if price is not None:
                                return float(price)
        
        # Try prices structure
        if "prices" in data:
            for variant, price_str in data["prices"].items():
                match = re.search(r'([0-9.]+)', str(price_str))
                if match:
                    return float(match.group(1))
        
        return 0.0
    
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
    
    # Print final summary
    print("\n" + "=" * 60)
    print("🎯 FINAL PRICE SUMMARY (Sorted by Price)")
    print("=" * 60)
    
    for item in combined["price_summary"]:
        if item["price"] is not None:
            print(f"   {item['provider']:25s} ${item['price']:.2f}/hr")
        else:
            print(f"   {item['provider']:25s} FAILED - {item['status']}")
    
    print(f"\n✅ Total providers: {combined['total_providers']}")
    print(f"   ✅ Successful: {combined['successful_fetches']}")
    print(f"   ❌ Failed: {combined['failed_fetches']}")
    
    # Price statistics for successful fetches
    successful_prices = [p for p in combined["price_summary"] if p["price"] is not None]
    if successful_prices:
        min_price = min(successful_prices, key=lambda x: x["price"])
        max_price = max(successful_prices, key=lambda x: x["price"])
        avg_price = sum(p["price"] for p in successful_prices) / len(successful_prices)
        
        print(f"\n📊 Price Statistics (successful fetches only):")
        print(f"   Lowest:  {min_price['provider']} at ${min_price['price']:.2f}/hr")
        print(f"   Highest: {max_price['provider']} at ${max_price['price']:.2f}/hr")
        print(f"   Average: ${avg_price:.2f}/hr")
    
    print(f"\n⏱️  Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
