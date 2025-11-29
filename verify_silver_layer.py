"""
Script to verify Silver layer data and monitor streaming job
"""

import os
import time
from datetime import datetime

def check_directories():
    """Check if output directories exist and show structure"""
    print("=" * 80)
    print("📁 CHECKING OUTPUT DIRECTORIES")
    print("=" * 80)
    
    checkpoint_dir = "/tmp/spark-checkpoints/silver"
    output_dir = "/tmp/silver_layer"
    
    print(f"\n1. Checkpoint Directory: {checkpoint_dir}")
    if os.path.exists(checkpoint_dir):
        print("   ✅ EXISTS")
        # Count files
        total_files = sum([len(files) for r, d, files in os.walk(checkpoint_dir)])
        print(f"   📊 Total files: {total_files}")
    else:
        print("   ❌ NOT FOUND")
    
    print(f"\n2. Silver Layer Output: {output_dir}")
    if os.path.exists(output_dir):
        print("   ✅ EXISTS")
        # Show partition structure
        try:
            partitions = [d for d in os.listdir(output_dir) if d.startswith('event_date=')]
            print(f"   📊 Partitions found: {len(partitions)}")
            for partition in partitions[:5]:  # Show first 5
                print(f"      - {partition}")
                sub_path = os.path.join(output_dir, partition)
                if os.path.isdir(sub_path):
                    event_types = [d for d in os.listdir(sub_path) if d.startswith('event_type=')]
                    for et in event_types[:3]:
                        print(f"         └─ {et}")
        except Exception as e:
            print(f"   ⚠️  Error listing partitions: {e}")
    else:
        print("   ❌ NOT FOUND")
    
    print("\n" + "=" * 80)

def count_parquet_files():
    """Count parquet files in Silver layer"""
    print("\n📊 COUNTING PARQUET FILES")
    print("=" * 80)
    
    output_dir = "/tmp/silver_layer"
    if not os.path.exists(output_dir):
        print("❌ Silver layer directory not found")
        return
    
    parquet_count = 0
    for root, dirs, files in os.walk(output_dir):
        parquet_files = [f for f in files if f.endswith('.parquet')]
        if parquet_files:
            print(f"\n📂 {root}")
            print(f"   Parquet files: {len(parquet_files)}")
            parquet_count += len(parquet_files)
    
    print(f"\n✅ Total Parquet files: {parquet_count}")
    print("=" * 80)

def show_monitoring_info():
    """Display monitoring information"""
    print("\n" + "=" * 80)
    print("🔍 MONITORING INFORMATION")
    print("=" * 80)
    
    print("\n📊 Spark Master UI:")
    print("   URL: http://localhost:8080")
    print("   - View running applications")
    print("   - Check worker status")
    print("   - Monitor resource usage")
    
    print("\n📈 Streaming Query Monitoring:")
    print("   - Check console output for real-time events")
    print("   - Look for 'Batch:' messages indicating processing")
    print("   - Monitor processing time and records processed")
    
    print("\n🐳 Docker Commands:")
    print("   View Spark logs:")
    print("   docker logs spark-master")
    print("   docker logs spark-worker-1")
    
    print("\n📁 File System Check:")
    print("   List Silver layer files:")
    print("   docker exec spark-master ls -lah /tmp/silver_layer/")
    
    print("\n" + "=" * 80)

def main():
    """Main verification function"""
    print("\n" + "=" * 80)
    print("🔍 SILVER LAYER VERIFICATION TOOL")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    check_directories()
    count_parquet_files()
    show_monitoring_info()
    
    print("\n✅ Verification complete!")
    print("\n💡 TIP: Run this script periodically to monitor data ingestion")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
