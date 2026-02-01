#!/usr/bin/env python3
"""
A100 GPU Index - Supabase Push Script

Pushes the calculated A100 index price to Supabase with validation:
- New price must be within ±20% of the last recorded price
- If validation fails, the push is rejected

Requirements:
    pip install supabase python-dotenv

Environment variables (create .env file):
    SUPABASE_URL=your_supabase_project_url
    SUPABASE_KEY=your_supabase_anon_key
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Try to import supabase
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    print("Warning: supabase package not installed. Run: pip install supabase")


class A100IndexPusher:
    """Push A100 index prices to Supabase with validation"""
    
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        self.table_name = "a100_index_prices"
        self.validation_threshold = 0.20  # ±20%
        self.client: Optional[Client] = None
        
        # Initialize Supabase client
        if SUPABASE_AVAILABLE and self.supabase_url and self.supabase_key:
            try:
                self.client = create_client(self.supabase_url, self.supabase_key)
                print("✓ Supabase client initialized")
            except Exception as e:
                print(f"✗ Failed to initialize Supabase client: {e}")
        elif not self.supabase_url or not self.supabase_key:
            print("⚠ Supabase credentials not found in environment variables")
            print("  Set SUPABASE_URL and SUPABASE_KEY in .env file")
    
    def load_index_data(self, filepath: str = "a100_weighted_index.json") -> Optional[Dict]:
        """Load the calculated index data from JSON file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"✓ Loaded index data from {filepath}")
                return data
        except FileNotFoundError:
            print(f"✗ Index file not found: {filepath}")
            return None
        except json.JSONDecodeError as e:
            print(f"✗ Invalid JSON in {filepath}: {e}")
            return None
    
    def get_last_price(self) -> Optional[float]:
        """Fetch the last recorded price from Supabase"""
        if not self.client:
            print("  Supabase client not available")
            return None
        
        try:
            response = self.client.table(self.table_name) \
                .select("index_price") \
                .order("created_at", desc=True) \
                .limit(1) \
                .execute()
            
            if response.data and len(response.data) > 0:
                last_price = float(response.data[0]["index_price"])
                print(f"  Last recorded price: ${last_price:.2f}/hr")
                return last_price
            else:
                print("  No previous price found (first entry)")
                return None
                
        except Exception as e:
            print(f"  Error fetching last price: {e}")
            return None
    
    def validate_price_change(self, new_price: float, last_price: Optional[float]) -> Tuple[bool, float]:
        """
        Validate that new price is within ±20% of last price
        
        Returns:
            Tuple of (is_valid, change_percent)
        """
        if last_price is None:
            # First entry - no validation needed
            return True, 0.0
        
        if last_price == 0:
            # Avoid division by zero
            return True, 0.0
        
        # Calculate percentage change
        change_percent = ((new_price - last_price) / last_price) * 100
        
        # Check if within ±20%
        is_valid = abs(change_percent) <= (self.validation_threshold * 100)
        
        return is_valid, change_percent
    
    def push_to_supabase(self, index_data: Dict, force: bool = False) -> bool:
        """
        Push index price to Supabase with validation
        
        Args:
            index_data: Dictionary containing the index calculation results
            force: If True, bypass validation and push anyway
            
        Returns:
            True if push was successful, False otherwise
        """
        if not self.client:
            print("✗ Cannot push: Supabase client not available")
            return False
        
        # Extract price from index data
        new_price = index_data.get("final_index_price")
        if new_price is None:
            print("✗ No index price found in data")
            return False
        
        print(f"\n📊 New A100 Index Price: ${new_price:.2f}/hr")
        
        # Get last price for validation
        last_price = self.get_last_price()
        
        # Validate price change
        is_valid, change_percent = self.validate_price_change(new_price, last_price)
        
        print(f"  Price change: {change_percent:+.2f}%")
        print(f"  Validation threshold: ±{self.validation_threshold * 100:.0f}%")
        
        if not is_valid and not force:
            print(f"\n⚠️  VALIDATION FAILED")
            print(f"   New price ${new_price:.2f} is {abs(change_percent):.1f}% different from last price ${last_price:.2f}")
            print(f"   This exceeds the ±20% threshold.")
            print(f"   Push rejected to prevent data anomalies.")
            print(f"\n   To force push anyway, use: push_to_supabase(data, force=True)")
            return False
        
        if not is_valid and force:
            print(f"\n⚠️  Force pushing despite validation failure")
        else:
            print(f"  ✓ Validation passed")
        
        # Prepare data for insertion
        insert_data = {
            "recorded_at": index_data.get("timestamp", datetime.now().isoformat()),
            "index_price": new_price,
            "hyperscaler_component": index_data.get("hyperscaler_component"),
            "neocloud_component": index_data.get("neocloud_component"),
            "hyperscaler_count": index_data.get("hyperscaler_count"),
            "neocloud_count": index_data.get("neocloud_count"),
            "previous_price": last_price,
            "price_change_percent": round(change_percent, 2),
            "validation_passed": is_valid,
            "raw_data": json.dumps(index_data)  # Store full data as JSON
        }
        
        try:
            response = self.client.table(self.table_name).insert(insert_data).execute()
            
            if response.data:
                print(f"\n✅ Successfully pushed to Supabase!")
                print(f"   Table: {self.table_name}")
                print(f"   Price: ${new_price:.2f}/hr")
                print(f"   ID: {response.data[0].get('id', 'N/A')}")
                return True
            else:
                print(f"✗ Push failed: No data returned")
                return False
                
        except Exception as e:
            print(f"✗ Push failed: {e}")
            return False
    
    def get_price_history(self, limit: int = 10) -> Optional[list]:
        """Fetch recent price history from Supabase"""
        if not self.client:
            return None
        
        try:
            response = self.client.table(self.table_name) \
                .select("recorded_at, index_price, price_change_percent, validation_passed") \
                .order("created_at", desc=True) \
                .limit(limit) \
                .execute()
            
            return response.data
            
        except Exception as e:
            print(f"Error fetching history: {e}")
            return None


def main():
    """Main function to push A100 index to Supabase"""
    print("=" * 60)
    print("A100 GPU Index - Supabase Push")
    print("=" * 60)
    
    # Initialize pusher
    pusher = A100IndexPusher()
    
    # Load index data
    index_file = Path(__file__).parent / "a100_weighted_index.json"
    index_data = pusher.load_index_data(str(index_file))
    
    if not index_data:
        print("\n❌ Cannot proceed without index data")
        return False
    
    print(f"\n📈 Index Summary:")
    print(f"   Final Price: ${index_data.get('final_index_price', 0):.2f}/hr")
    print(f"   Hyperscalers: {index_data.get('hyperscaler_count', 0)} providers")
    print(f"   Neoclouds: {index_data.get('neocloud_count', 0)} providers")
    
    # Push to Supabase with validation
    print("\n" + "-" * 60)
    print("Pushing to Supabase with ±20% validation...")
    print("-" * 60)
    
    success = pusher.push_to_supabase(index_data)
    
    # Show recent history
    if success and pusher.client:
        print("\n📜 Recent Price History:")
        history = pusher.get_price_history(5)
        if history:
            for entry in history:
                ts = entry.get("recorded_at", "N/A")
                price = entry.get("index_price", 0)
                change = entry.get("price_change_percent", 0)
                valid = "✓" if entry.get("validation_passed") else "✗"
                print(f"   {ts}: ${price:.2f} ({change:+.1f}%) {valid}")
    
    return success


if __name__ == "__main__":
    main()
