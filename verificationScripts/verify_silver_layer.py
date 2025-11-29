"""
Script to verify Silver layer data and monitor streaming job
"""

import subprocess
import json
from datetime import datetime

def run_docker_command(command):
    """Execute docker command and return output"""
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.stdout.strip(), result.returncode
    except Exception as e:
        return f"Error: {str(e)}", 1

def check_docker_services():
    """Check if required Docker services are running"""
    print("=" * 80)
    print("🐳 CHECKING DOCKER SERVICES")
    print("=" * 80)
    
    # Use docker ps without format for Windows compatibility
    output, code = run_docker_command("docker ps --format \"{{.Names}}\t{{.Status}}\"")
    
    if code == 0 and output:
        services = output.split('\n')
        required_services = ['kafka', 'zookeeper', 'spark-master', 'spark-worker']
        
        running_services = []
        for service in services:
            if service:
                parts = service.split('\t')
                if len(parts) >= 2:
                    name = parts[0]
                    status = parts[1]
                else:
                    name = service
                    status = 'Running'
                
                if any(req in name for req in required_services):
                    running_services.append(name)
                    print(f"✅ {name}: {status}")
        
        missing = [s for s in required_services if not any(s in rs for rs in running_services)]
        if missing:
            print(f"\n⚠️  Missing services: {', '.join(missing)}")
            return False
        return True
    else:
        print("❌ Cannot connect to Docker")
        print(f"Debug: code={code}, output={output[:100] if output else 'None'}")
        return False

def check_kafka_topic():
    """Check if Kafka topic exists and has data"""
    print("\n" + "=" * 80)
    print("📋 CHECKING KAFKA TOPIC")
    print("=" * 80)
    
    # List topics
    output, code = run_docker_command(
        "docker exec kafka kafka-topics --list --bootstrap-server localhost:9092"
    )
    
    if code == 0 and "ecom_clickstream_raw" in output:
        print("✅ Topic 'ecom_clickstream_raw' exists")
        
        # Get message count
        output, code = run_docker_command(
            "docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic ecom_clickstream_raw"
        )
        
        if code == 0:
            total_messages = 0
            for line in output.split('\n'):
                if ':' in line:
                    try:
                        offset = int(line.split(':')[-1])
                        total_messages += offset
                    except:
                        pass
            print(f"📊 Total messages in topic: {total_messages}")
            return total_messages > 0
        else:
            print("⚠️  Cannot get message count")
            return False
    else:
        print("❌ Topic 'ecom_clickstream_raw' not found")
        print("\n💡 Create it with:")
        print("   docker exec -it kafka kafka-topics --create --topic ecom_clickstream_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1")
        return False

def check_silver_layer():
    """Check Silver layer directories inside Docker container"""
    print("\n" + "=" * 80)
    print("📁 CHECKING SILVER LAYER (Inside Docker)")
    print("=" * 80)
    
    # Check if directory exists
    output, code = run_docker_command(
        "docker exec spark-master ls /tmp/silver_layer 2>/dev/null"
    )
    
    if code == 0 and output:
        print("✅ Silver layer directory exists: /tmp/silver_layer")
        
        # Show structure
        output, code = run_docker_command(
            "docker exec spark-master ls -R /tmp/silver_layer"
        )
        
        if code == 0:
            print("\n📂 Directory structure:")
            print(output)
        
        # Count parquet files
        output, code = run_docker_command(
            'docker exec spark-master find /tmp/silver_layer -name "*.parquet" -type f'
        )
        
        if code == 0:
            parquet_files = [f for f in output.split('\n') if f.strip()]
            print(f"\n📊 Total Parquet files: {len(parquet_files)}")
            
            if len(parquet_files) > 0:
                print("\n📄 Sample files:")
                for f in parquet_files[:5]:
                    print(f"   - {f}")
                return True
        else:
            print("⚠️  No Parquet files found")
            return False
    else:
        print("❌ Silver layer directory not found")
        print("\n💡 The Spark streaming job may not be running")
        return False

def check_spark_application():
    """Check if Spark streaming application is running"""
    print("\n" + "=" * 80)
    print("⚡ CHECKING SPARK APPLICATION")
    print("=" * 80)
    
    # Check inside Docker container
    output, code = run_docker_command(
        "docker exec spark-master ps aux"
    )
    
    if code == 0 and "spark_streaming_silver.py" in output:
        print("✅ Spark streaming job is running inside container")
        
        # Try to get app info
        ps_output, _ = run_docker_command(
            "docker exec spark-master ps aux | findstr spark-submit"
        )
        if ps_output:
            print(f"📊 Process info: {ps_output.split()[0:11]}")
        return True
    else:
        print("❌ Spark streaming job is NOT running")
        print("\n💡 Start it with: submit_spark_job.bat")
        return False

def show_monitoring_info():
    """Display monitoring commands"""
    print("\n" + "=" * 80)
    print("🔍 USEFUL MONITORING COMMANDS")
    print("=" * 80)
    
    print("\n📊 Web Interfaces:")
    print("   Spark Master:  http://localhost:8080")
    print("   Spark App:     http://localhost:4040 (when job running)")
    
    print("\n📋 Check Kafka:")
    print("   docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092")
    print("   docker exec -it kafka kafka-console-consumer --topic ecom_clickstream_raw --from-beginning --bootstrap-server localhost:9092 --max-messages 5")
    
    print("\n📁 Check Silver Layer:")
    print("   docker exec spark-master ls -lah /tmp/silver_layer/")
    print("   docker exec spark-master find /tmp/silver_layer -name '*.parquet' -type f")
    
    print("\n📝 View Spark Logs:")
    print("   docker logs spark-master --tail 50")
    
    print("\n🔄 View Producer Output:")
    print("   python ecom_producer.py")

def main():
    """Main verification function"""
    print("\n" + "=" * 80)
    print("🔍 SILVER LAYER VERIFICATION TOOL")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Run checks
    docker_ok = check_docker_services()
    
    if not docker_ok:
        print("\n❌ Docker services not running properly")
        print("💡 Run: docker-compose up -d")
        return
    
    kafka_ok = check_kafka_topic()
    silver_ok = check_silver_layer()
    spark_ok = check_spark_application()
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 80)
    print(f"Docker Services:  {'✅' if docker_ok else '❌'}")
    print(f"Kafka Topic:      {'✅' if kafka_ok else '❌'}")
    print(f"Silver Layer:     {'✅' if silver_ok else '❌'}")
    print(f"Spark Job:        {'✅' if spark_ok else '❌'}")
    
    if docker_ok and kafka_ok and not silver_ok:
        print("\n💡 NEXT STEPS:")
        print("   1. Start producer: python ecom_producer.py")
        print("   2. Start Spark job: submit_spark_job.bat")
        print("   3. Wait 30 seconds and run this script again")
    
    show_monitoring_info()
    
    print("\n" + "=" * 80)
    print("✅ Verification complete!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()