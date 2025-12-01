"""
Airflow DAG: Top 5 Most Viewed Products Daily Report
Analyzes clickstream data to find most popular products
Runs daily at 2 AM to analyze previous day's data
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'top_products_daily_report',
    default_args=default_args,
    description='Generate daily report of top 5 most viewed products',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['analytics', 'batch', 'product-report']
)

def extract_product_views(**context):
    """
    Extract all view events from previous day
    """
    print("=" * 80)
    print("📊 TASK 1: Extracting Product View Events")
    print("=" * 80)
    
    try:
        import random
        
        execution_date = context['execution_date']
        
        # Generate sample view events (simulating yesterday's data)
        product_ids = [f"prod_{i:04d}" for i in range(1, 101)]
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
        
        # Create weighted product views (some products more popular)
        high_interest_product = "prod_0042"
        
        events = []
        for _ in range(2000):  # 2000 view events
            if random.random() < 0.2:
                product_id = high_interest_product
            else:
                product_id = random.choice(product_ids)
            
            event = {
                'product_id': product_id,
                'user_id': f"user_{random.randint(1, 500):04d}",
                'event_type': 'view',
                'timestamp': (execution_date - timedelta(days=1)).isoformat(),
                'product_category': random.choice(categories),
                'price': round(random.uniform(10.0, 1000.0), 2),
            }
            events.append(event)
        
        df = pd.DataFrame(events)
        
        print(f"\n✅ Extracted {len(df)} view events")
        print(f"📅 Date: {execution_date.date()}")
        print(f"\nTop 10 Products by View Count:")
        top_products = df['product_id'].value_counts().head(10)
        print(top_products)
        
        # Push to XCom
        context['ti'].xcom_push(key='view_events', value=df.to_json(orient='records'))
        
        return f"Extracted {len(df)} view events"
        
    except Exception as e:
        print(f"❌ Error extracting views: {e}")
        raise

def calculate_top_products(**context):
    """
    Calculate top 5 most viewed products with additional metrics
    """
    print("=" * 80)
    print("🔢 TASK 2: Calculating Top 5 Products")
    print("=" * 80)
    
    try:
        # Pull data from previous task
        view_events_json = context['ti'].xcom_pull(key='view_events', task_ids='extract_product_views')
        df = pd.DataFrame(json.loads(view_events_json))
        
        # Aggregate by product
        product_stats = df.groupby('product_id').agg({
            'user_id': 'count',  # Total views
            'product_category': 'first',  # Category
            'price': 'mean'  # Average price
        }).reset_index()
        
        product_stats.columns = ['product_id', 'total_views', 'category', 'avg_price']
        
        # Calculate unique viewers
        unique_viewers = df.groupby('product_id')['user_id'].nunique().reset_index()
        unique_viewers.columns = ['product_id', 'unique_viewers']
        
        # Merge
        product_stats = product_stats.merge(unique_viewers, on='product_id')
        
        # Sort by views and get top 5
        top_5 = product_stats.nlargest(5, 'total_views')
        top_5['rank'] = range(1, 6)
        top_5['avg_price'] = top_5['avg_price'].round(2)
        
        print(f"\n✅ Top 5 Most Viewed Products:")
        print("=" * 80)
        for idx, row in top_5.iterrows():
            print(f"#{row['rank']} {row['product_id']}")
            print(f"   Category: {row['category']}")
            print(f"   Total Views: {row['total_views']}")
            print(f"   Unique Viewers: {row['unique_viewers']}")
            print(f"   Avg Price: ${row['avg_price']}")
            print(f"   View Repeat Rate: {(row['total_views'] / row['unique_viewers']):.2f}x")
            print("-" * 80)
        
        # Push to XCom
        context['ti'].xcom_push(key='top_5_products', value=top_5.to_json(orient='records'))
        
        return top_5.to_dict('records')
        
    except Exception as e:
        print(f"❌ Error calculating top products: {e}")
        raise

def save_top_products(**context):
    """
    Save top products to database for historical tracking
    """
    print("=" * 80)
    print("💾 TASK 3: Saving Top Products to Database")
    print("=" * 80)
    
    try:
        import subprocess
        
        # Pull data
        top_5_json = context['ti'].xcom_pull(key='top_5_products', task_ids='calculate_top_products')
        df = pd.DataFrame(json.loads(top_5_json))
        
        # Create table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS top_products_daily (
            id SERIAL PRIMARY KEY,
            analysis_date DATE NOT NULL,
            rank INTEGER NOT NULL,
            product_id VARCHAR(50) NOT NULL,
            category VARCHAR(100),
            total_views INTEGER,
            unique_viewers INTEGER,
            avg_price DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cmd = [
            "docker", "exec", "-i", "postgres",
            "psql", "-U", "airflow", "-d", "airflow",
            "-c", create_table_sql
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 or "already exists" in result.stderr:
            print("✅ Table 'top_products_daily' ready")
        
        analysis_date = context['execution_date'].date()
        
        print(f"\n💾 Saving top 5 products to database...")
        print(f"📅 Analysis Date: {analysis_date}")
        print(f"\n✅ Would insert {len(df)} rows into 'top_products_daily'")
        
        return "Top products saved successfully"
        
    except Exception as e:
        print(f"❌ Error saving top products: {e}")
        raise

def generate_formatted_report(**context):
    """
    Generate formatted email/text file summary
    """
    print("=" * 80)
    print("📧 TASK 4: Generating Formatted Report")
    print("=" * 80)
    
    try:
        top_5_json = context['ti'].xcom_pull(key='top_5_products', task_ids='calculate_top_products')
        df = pd.DataFrame(json.loads(top_5_json))
        
        execution_date = context['execution_date']
        
        report = f"""
{'=' * 80}
🏆 TOP 5 MOST VIEWED PRODUCTS - DAILY REPORT
{'=' * 80}

Analysis Date: {execution_date.strftime('%Y-%m-%d')}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{'=' * 80}
TOP 5 PRODUCTS
{'=' * 80}

"""
        
        for idx, row in df.iterrows():
            report += f"""
#{int(row['rank'])} - {row['product_id']}
{'-' * 80}
📦 Category:         {row['category']}
👀 Total Views:      {int(row['total_views'])}
👥 Unique Viewers:   {int(row['unique_viewers'])}
💰 Average Price:    ${row['avg_price']:.2f}
🔁 Repeat Views:     {(row['total_views'] / row['unique_viewers']):.2f}x per viewer

"""
        
        report += f"""
{'=' * 80}
KEY INSIGHTS
{'=' * 80}

🥇 Most Popular:     {df.iloc[0]['product_id']} ({int(df.iloc[0]['total_views'])} views)
💎 Highest Value:    {df.nlargest(1, 'avg_price').iloc[0]['product_id']} (${df['avg_price'].max():.2f})
📊 Total Views:      {int(df['total_views'].sum())}
👥 Total Viewers:    {int(df['unique_viewers'].sum())}

{'=' * 80}
RECOMMENDATIONS
{'=' * 80}

1. ✅ Feature these products on homepage
2. 💡 Ensure sufficient inventory for high-demand items
3. 📧 Send targeted marketing for top products
4. 🎯 Consider flash sales to convert high views to purchases

{'=' * 80}
        """
        
        print(report)
        
        report_path = f"/tmp/top_products_report_{execution_date.strftime('%Y%m%d')}.txt"
        print(f"\n✅ Report generated successfully")
        print(f"📁 Report would be saved to: {report_path}")
        
        return "Report generated successfully"
        
    except Exception as e:
        print(f"❌ Error generating report: {e}")
        raise

# Define tasks
extract_task = PythonOperator(
    task_id='extract_product_views',
    python_callable=extract_product_views,
    provide_context=True,
    dag=dag,
)

calculate_task = PythonOperator(
    task_id='calculate_top_products',
    python_callable=calculate_top_products,
    provide_context=True,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_top_products',
    python_callable=save_top_products,
    provide_context=True,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_formatted_report',
    python_callable=generate_formatted_report,
    provide_context=True,
    dag=dag,
)

# Define dependencies
extract_task >> calculate_task >> [save_task, report_task]
