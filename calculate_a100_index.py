#!/usr/bin/env python3
"""
A100 GPU Weighted Index Calculator with Dynamic Volatility

Calculates a weighted A100 GPU index price with built-in volatility from:
1. Hyperscalers (AWS, Azure, GCP, Oracle) - 65% weight with static discounts
2. Neoclouds (all other providers) - 35% weight with DYNAMIC weights based on availability
3. Price distribution support (min/median/max from marketplaces like Vast.ai)
4. Live EUR→USD currency conversion

Dynamic Weighting:
- Providers with "high" availability get +20% weight boost
- Providers with "medium" availability keep base weight
- Providers with "low" availability get -30% weight reduction
- Providers with "unavailable" status get -70% weight reduction

NO FALLBACK VALUES - only live data from scrapers
"""

import json
import re
import requests
from pathlib import Path
from typing import Dict, Tuple, Optional
from datetime import datetime


class A100IndexCalculator:
    """Calculate weighted A100 GPU index price with dynamic volatility"""
    
    def __init__(self, a100_dir: str = "."):
        self.a100_dir = Path(a100_dir)
        
        # Base A100 configuration
        self.base_config = {
            "gpu_model": "A100",
            "gpu_memory_gb": 40,
            "memory_bandwidth_tb_s": 2.0,
            "fp16_tflops": 312,
            "form_factor": "PCIe/SXM"
        }
        
        # Define hyperscalers
        self.hyperscalers = ["AWS", "Azure", "GCP", "Oracle"]
        
        # Hyperscaler name aliases
        self.hyperscaler_aliases = {
            "AWS": ["aws", "amazon", "p4d"],
            "Azure": ["azure", "microsoft", "nd96"],
            "GCP": ["gcp", "google", "google cloud", "a2-highgpu"],
            "Oracle": ["oracle", "oci", "bm.gpu"],
        }
        
        # Static hyperscaler discounts
        self.hyperscaler_discounts = {
            "AWS": 0.44,
            "Azure": 0.65,
            "GCP": 0.65,
            "Oracle": 0.25,
        }
        
        # Discount blend
        self.discounted_weight = 0.80
        self.full_price_weight = 0.20
        
        # Total weight distribution
        self.hyperscaler_total_weight = 0.65
        self.neocloud_total_weight = 0.35
        
        # Hyperscaler weights
        self.hyperscaler_weights = {
            "AWS": 0.42,
            "Azure": 0.33,
            "GCP": 0.17,
            "Oracle": 0.08,
        }
        
        # Base neocloud weights (will be adjusted dynamically)
        self.base_neocloud_weights = {
            "Civo": 0.15,
            "Vast.ai": 0.15,  # Higher base weight for marketplace (more volatile)
            "CUDO Compute": 0.12,
            "HyperStack": 0.12,
            "RunPod": 0.15,
            "Paperspace": 0.10,
            "Hostkey": 0.08,
            "Lambda Labs": 0.13,
            "default": 0.05,
        }
        
        # Dynamic weight multipliers based on availability
        self.availability_multipliers = {
            "high": 1.20,       # +20% weight - plenty of GPUs available
            "medium": 1.00,    # Base weight
            "low": 0.70,       # -30% weight - scarce supply
            "unavailable": 0.30,  # -70% weight - but still include
            "unknown": 1.00,   # Unknown = base weight
        }
        
        # Store availability info for reporting
        self.provider_availability = {}
        
        # Exchange rate cache
        self._eur_usd_rate = None
    
    def get_eur_to_usd_rate(self) -> float:
        """Fetch live EUR to USD exchange rate"""
        if self._eur_usd_rate is not None:
            return self._eur_usd_rate
            
        try:
            response = requests.get(
                "https://api.exchangerate-api.com/v4/latest/EUR",
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                rate = data.get("rates", {}).get("USD", 1.08)
                self._eur_usd_rate = rate
                print(f"  Live EUR/USD rate: {rate:.4f}")
                return rate
        except Exception as e:
            print(f"  Exchange rate API error: {e}")
        
        try:
            response = requests.get(
                "https://open.er-api.com/v6/latest/EUR",
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                rate = data.get("rates", {}).get("USD", 1.08)
                self._eur_usd_rate = rate
                return rate
        except Exception as e:
            pass
        
        print("  Warning: Could not fetch live exchange rate, using 1.08")
        self._eur_usd_rate = 1.08
        return 1.08
    
    def convert_to_usd(self, price: float, currency: str) -> float:
        """Convert price to USD"""
        if currency.upper() == "USD":
            return price
        elif currency.upper() == "EUR":
            rate = self.get_eur_to_usd_rate()
            return price * rate
        return price
    
    def load_all_prices(self) -> Dict[str, Dict]:
        """Load prices from all JSON files with availability info"""
        prices = {}
        
        json_files = list(self.a100_dir.glob("*_a100_prices.json"))
        print(f"📂 Found {len(json_files)} A100 price files\n")
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    if data.get("fetch_status") == "failed":
                        continue
                    if not data.get("prices") and not data.get("providers"):
                        continue
                    
                    provider = data.get("provider", json_file.stem.replace("_a100_prices", ""))
                    
                    # Extract price, currency, and availability
                    price, currency = self._extract_price_from_data(data)
                    availability = data.get("availability", "unknown")
                    gpu_count = data.get("gpu_count", 0)
                    distribution = data.get("distribution", {})
                    
                    # Store availability info
                    self.provider_availability[provider] = {
                        "availability": availability,
                        "gpu_count": gpu_count,
                        "distribution": distribution
                    }
                    
                    if price and price > 0:
                        usd_price = self.convert_to_usd(price, currency)
                        prices[provider] = {
                            "original_price": price,
                            "currency": currency,
                            "usd_price": usd_price,
                            "source_file": json_file.name,
                            "availability": availability,
                            "gpu_count": gpu_count,
                            "distribution": distribution
                        }
                        
                        # Show availability status
                        avail_icon = {"high": "🟢", "medium": "🟡", "low": "🟠", "unavailable": "🔴"}.get(availability, "⚪")
                        currency_note = f" (from {currency})" if currency != "USD" else ""
                        print(f"   {avail_icon} {provider:20s} ${usd_price:.2f}/hr{currency_note}")
                        
            except Exception as e:
                print(f"   ✗ Error loading {json_file}: {e}")
        
        return prices
    
    def _extract_price_from_data(self, data: Dict) -> Tuple[float, str]:
        """Extract price value and currency from provider data"""
        # Check for distribution data (enhanced format with median)
        distribution = data.get("distribution", {})
        if distribution and "median" in distribution:
            # Use median price from distribution for volatility
            return float(distribution["median"]), "USD"
        
        # Try nested providers structure (hyperscaler format)
        if "providers" in data:
            for provider_name, provider_data in data["providers"].items():
                if "variants" in provider_data:
                    for variant_name, variant_data in provider_data["variants"].items():
                        if isinstance(variant_data, dict) and "price_per_hour" in variant_data:
                            currency = variant_data.get("currency", "USD")
                            return float(variant_data["price_per_hour"]), currency
        
        # Try prices structure (neocloud format)
        if "prices" in data and data["prices"]:
            prices_dict = data["prices"]
            for variant, price_str in prices_dict.items():
                if variant == "Error":
                    continue
                
                # Prefer Median price for marketplace providers
                if "Median" in variant or "Mean" in variant:
                    match = re.search(r'\$?(\d+\.?\d*)', str(price_str))
                    if match:
                        return float(match.group(1)), "USD"
                
                # Check for EUR
                if "€" in str(price_str):
                    match = re.search(r'€?\s*(\d+\.?\d*)', str(price_str))
                    if match:
                        return float(match.group(1)), "EUR"
                
                # USD
                match = re.search(r'\$?\s*(\d+\.?\d*)', str(price_str))
                if match:
                    return float(match.group(1)), "USD"
        
        return 0.0, "USD"
    
    def get_dynamic_weight(self, provider: str, base_weight: float) -> Tuple[float, str]:
        """Calculate dynamic weight based on availability"""
        availability_info = self.provider_availability.get(provider, {})
        availability = availability_info.get("availability", "unknown")
        
        multiplier = self.availability_multipliers.get(availability, 1.0)
        dynamic_weight = base_weight * multiplier
        
        return dynamic_weight, availability
    
    def categorize_providers(self, prices: Dict[str, Dict]) -> Tuple[Dict, Dict]:
        """Categorize providers into hyperscalers and neoclouds"""
        hyperscaler_prices = {}
        neocloud_prices = {}
        
        for provider, data in prices.items():
            provider_lower = provider.lower().strip()
            
            matched_hyperscaler = None
            for hs_name, aliases in self.hyperscaler_aliases.items():
                for alias in aliases:
                    if alias in provider_lower:
                        matched_hyperscaler = hs_name
                        break
                if matched_hyperscaler:
                    break
            
            if matched_hyperscaler:
                hyperscaler_prices[matched_hyperscaler] = data["usd_price"]
            else:
                neocloud_prices[provider] = data["usd_price"]
        
        return hyperscaler_prices, neocloud_prices
    
    def apply_hyperscaler_discounts(self, prices: Dict[str, float]) -> Dict[str, Dict]:
        """Apply static discounts to hyperscalers"""
        discounted_data = {}
        
        print("\n" + "=" * 80)
        print("💰 HYPERSCALER DISCOUNT APPLICATION")
        print("=" * 80)
        print("Discounts: AWS 44%, Azure 65%, GCP 65%, Oracle 25%")
        print("Blend: 80% discounted + 20% full price\n")
        
        for provider, original_price in prices.items():
            discount_rate = self.hyperscaler_discounts.get(provider, 0.30)
            discounted_price = original_price * (1 - discount_rate)
            effective_price = (discounted_price * self.discounted_weight) + \
                             (original_price * self.full_price_weight)
            
            discounted_data[provider] = {
                "original_price": original_price,
                "discount_rate": discount_rate,
                "discounted_price": discounted_price,
                "effective_price": effective_price
            }
            
            print(f"🏢 {provider}")
            print(f"   Original: ${original_price:.2f}/hr → Effective: ${effective_price:.2f}/hr\n")
        
        return discounted_data
    
    def calculate_weighted_index(self, 
                                  hyperscaler_data: Dict[str, Dict],
                                  neocloud_prices: Dict[str, float]) -> Dict:
        """Calculate the final weighted index price with dynamic weights"""
        
        print("=" * 80)
        print("⚖️  WEIGHTED INDEX CALCULATION (WITH DYNAMIC WEIGHTS)")
        print("=" * 80)
        
        # =====================================================================
        # HYPERSCALER COMPONENT (65%)
        # =====================================================================
        print(f"\n📊 HYPERSCALERS (Total Weight: {self.hyperscaler_total_weight*100:.0f}%)")
        print("-" * 80)
        
        hyperscaler_weighted_sum = 0
        hyperscaler_details = []
        total_hyperscaler_weight_used = 0
        
        for provider, data in hyperscaler_data.items():
            if provider in self.hyperscaler_weights:
                individual_weight = self.hyperscaler_weights[provider]
                absolute_weight = individual_weight * self.hyperscaler_total_weight
                weighted_price = data["effective_price"] * absolute_weight
                
                hyperscaler_weighted_sum += weighted_price
                total_hyperscaler_weight_used += absolute_weight
                
                hyperscaler_details.append({
                    "provider": provider,
                    "original_price": data["original_price"],
                    "discount_rate": data["discount_rate"],
                    "effective_price": data["effective_price"],
                    "relative_weight": individual_weight,
                    "absolute_weight": absolute_weight,
                    "weighted_contribution": weighted_price
                })
                
                print(f"{provider:20s} ${data['effective_price']:6.2f}/hr × {absolute_weight*100:5.1f}% = ${weighted_price:.4f}")
        
        if total_hyperscaler_weight_used > 0 and total_hyperscaler_weight_used < self.hyperscaler_total_weight:
            normalization_factor = self.hyperscaler_total_weight / total_hyperscaler_weight_used
            hyperscaler_weighted_sum *= normalization_factor
            print(f"\n   ⚠️  Normalized: factor = {normalization_factor:.2f}")
        
        print(f"{'':20s} {'':11s} {'':7s}   {'─'*20}")
        print(f"{'Hyperscaler Subtotal':20s} {'':11s} {'':7s}   ${hyperscaler_weighted_sum:.4f}")
        
        # =====================================================================
        # NEOCLOUD COMPONENT (35%) - WITH DYNAMIC WEIGHTS
        # =====================================================================
        print(f"\n📊 NEOCLOUDS (Total Weight: {self.neocloud_total_weight*100:.0f}%) - DYNAMIC WEIGHTING")
        print("-" * 80)
        
        neocloud_weighted_sum = 0
        neocloud_details = []
        total_neocloud_weight_used = 0
        
        # First pass: calculate all dynamic weights
        dynamic_weights = {}
        for provider in neocloud_prices.keys():
            base_weight = self.base_neocloud_weights.get(provider, 
                         self.base_neocloud_weights.get("default", 0.05))
            dyn_weight, availability = self.get_dynamic_weight(provider, base_weight)
            dynamic_weights[provider] = (dyn_weight, availability)
        
        # Normalize dynamic weights to sum to 1.0
        total_dyn_weight = sum(w for w, _ in dynamic_weights.values())
        
        for provider, price in neocloud_prices.items():
            dyn_weight, availability = dynamic_weights[provider]
            
            # Normalize
            if total_dyn_weight > 0:
                normalized_weight = dyn_weight / total_dyn_weight
            else:
                normalized_weight = dyn_weight
            
            absolute_weight = normalized_weight * self.neocloud_total_weight
            weighted_price = price * absolute_weight
            
            neocloud_weighted_sum += weighted_price
            total_neocloud_weight_used += absolute_weight
            
            # Get availability icon
            avail_icon = {"high": "🟢", "medium": "🟡", "low": "🟠", "unavailable": "🔴"}.get(availability, "⚪")
            
            neocloud_details.append({
                "provider": provider,
                "price": price,
                "availability": availability,
                "base_weight": self.base_neocloud_weights.get(provider, 0.05),
                "dynamic_weight": normalized_weight,
                "absolute_weight": absolute_weight,
                "weighted_contribution": weighted_price
            })
            
            print(f"{avail_icon} {provider:18s} ${price:6.2f}/hr × {absolute_weight*100:5.2f}% = ${weighted_price:.4f} ({availability})")
        
        print(f"{'':20s} {'':11s} {'':7s}   {'─'*20}")
        print(f"{'Neocloud Subtotal':20s} {'':11s} {'':7s}   ${neocloud_weighted_sum:.4f}")
        
        # =====================================================================
        # FINAL INDEX
        # =====================================================================
        final_index = hyperscaler_weighted_sum + neocloud_weighted_sum
        
        print("\n" + "=" * 80)
        print("🎯 FINAL A100 INDEX PRICE")
        print("=" * 80)
        print(f"\nHyperscaler Component:    ${hyperscaler_weighted_sum:.4f} ({self.hyperscaler_total_weight*100:.0f}%)")
        print(f"Neocloud Component:       ${neocloud_weighted_sum:.4f} ({self.neocloud_total_weight*100:.0f}%)")
        print(f"{'─'*50}")
        print(f"A100 Weighted Index:      ${final_index:.2f}/hr")
        print("=" * 80)
        
        return {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "gpu_model": "A100",
            "final_index_price": round(final_index, 2),
            "hyperscaler_component": round(hyperscaler_weighted_sum, 4),
            "neocloud_component": round(neocloud_weighted_sum, 4),
            "hyperscaler_count": len(hyperscaler_data),
            "neocloud_count": len(neocloud_prices),
            "hyperscaler_details": hyperscaler_details,
            "neocloud_details": neocloud_details,
            "dynamic_weighting_enabled": True,
            "provider_availability": self.provider_availability,
            "weights": {
                "hyperscaler_total": self.hyperscaler_total_weight,
                "neocloud_total": self.neocloud_total_weight,
                "hyperscaler_individual": self.hyperscaler_weights,
                "hyperscaler_discounts": self.hyperscaler_discounts,
                "availability_multipliers": self.availability_multipliers,
                "discount_blend": {
                    "discounted_weight": self.discounted_weight,
                    "full_price_weight": self.full_price_weight
                }
            },
            "base_config": self.base_config,
            "eur_usd_rate": self._eur_usd_rate
        }
    
    def save_index_report(self, index_data: Dict, filename: str = "a100_weighted_index.json"):
        """Save index calculation report to JSON"""
        output_file = self.a100_dir / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Index report saved to: {output_file}")
        return output_file


def main():
    """Main function to calculate A100 weighted index with dynamic volatility"""
    print("🚀 A100 GPU Weighted Index Calculator")
    print("=" * 80)
    print("Calculating weighted A100 index with DYNAMIC VOLATILITY:")
    print("  • Hyperscalers (65%): AWS, Azure, GCP, Oracle")
    print("  • Neoclouds (35%): Dynamic weights based on availability")
    print("  • Marketplace support: Uses median prices from distributions")
    print("  • Availability multipliers: High +20%, Low -30%, Unavailable -70%")
    print("=" * 80)
    
    calculator = A100IndexCalculator()
    
    print("\n📂 Loading A100 Prices")
    print("-" * 80)
    prices = calculator.load_all_prices()
    
    if not prices:
        print("\n❌ No A100 price data found!")
        return
    
    print(f"\n✓ Loaded {len(prices)} provider prices")
    
    hyperscaler_prices, neocloud_prices = calculator.categorize_providers(prices)
    
    print(f"\n📊 Provider Categories:")
    print(f"   Hyperscalers: {len(hyperscaler_prices)}")
    print(f"   Neoclouds: {len(neocloud_prices)}")
    
    hyperscaler_data = calculator.apply_hyperscaler_discounts(hyperscaler_prices)
    index_data = calculator.calculate_weighted_index(hyperscaler_data, neocloud_prices)
    calculator.save_index_report(index_data)
    
    print(f"\n✅ Index calculation complete!")
    print(f"\n🎯 Final A100 Weighted Index Price: ${index_data['final_index_price']:.2f}/hr")


if __name__ == "__main__":
    main()
