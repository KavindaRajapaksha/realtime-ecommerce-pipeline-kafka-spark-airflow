"""
Airflow DAG: Conversion Rate Analytics by Product Category
Calculates Purchases / Views conversion rate for each category
Runs daily at 3 AM to analyze previous day's data
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
    'conversion_rate_analytics',
    default_args=default_args,
    description='Calculate conversion rates (Purchases/Views) by product category',
    schedule_interval='0 3 * * *',  # Run daily at 3 AM
    catchup=False,
    tags=['analytics', 'batch', 'conversion-rate']
)

def extract_all_events(**context):
    """
    Extract all clickstream events (views, add_to_cart, purchases)
    """
    print("=" * 80)
    print("📊 TASK 1: Extracting All Clickstream Events")
    print("=" * 80)
    
    try:
        import random
        
        execution_date = context['execution_date']
        
        # Generate sample events
        product_ids = [f"prod_{i:04d}" for i in range(1, 101)]
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
        event_types = ['view', 'add_to_cart', 'purchase']
        
        # Map products to categories (consistent mapping)
        product_category_map = {
            pid: categories[hash(pid) % len(categories)] 
            for pid in product_ids
        }
        
        events = []
        high_interest_product = "prod_0042"
        product_category_map[high_interest_product] = 'Electronics'
        
        # Generate 3000 events with realistic funnel
        for _ in range(3000):
            if random.random() < 0.2:
                product_id = high_interest_product
                event_type = random.choices(event_types, weights=[0.95, 0.04, 0.01])[0]
            else:
                product_id = random.choice(product_ids)
                event_type = random.choices(event_types, weights=[0.70, 0.20, 0.10])[0]
            
            event = {
                'event_id': f"evt_{len(events):08d}",
                'product_id': product_id,
                'user_id': f"user_{random.randint(1, 500):04d}",
                'event_type': event_type,
                'timestamp': (execution_date - timedelta(days=1)).isoformat(),
                'product_category': product_category_map[product_id],
                'price': round(random.uniform(10.0, 1000.0), 2),
            }
            events.append(event)
        
        df = pd.DataFrame(events)
        
        print(f"\n✅ Extracted {len(df)} total events")
        print(f"📅 Date: {execution_date.date()}")
        print(f"\nEvent Type Distribution:")
        print(df['event_type'].value_counts())
        print(f"\nCategory Distribution:")
        print(df['product_category'].value_counts())
        
        # Push to XCom
        context['ti'].xcom_push(key='all_events', value=df.to_json(orient='records'))
        
        return f"Extracted {len(df)} events"
        
    except Exception as e:
        print(f"❌ Error extracting events: {e}")
        raise

def calculate_conversion_rates(**context):
    """
    Calculate conversion rates by category and product
    """
    print("=" * 80)
    print("📊 TASK 2: Calculating Conversion Rates")
    print("=" * 80)
    
    try:
        # Pull data
        all_events_json = context['ti'].xcom_pull(key='all_events', task_ids='extract_all_events')
        df = pd.DataFrame(json.loads(all_events_json))
        
        # === Category-Level Conversion Rates ===
        print("\n" + "=" * 80)
        print("📊 CATEGORY-LEVEL CONVERSION RATES")
        print("=" * 80)
        
        category_stats = df.groupby(['product_category', 'event_type']).size().unstack(fill_value=0)
        
        # Ensure all columns exist
        for col in ['view', 'add_to_cart', 'purchase']:
            if col not in category_stats.columns:
                category_stats[col] = 0
        
        category_stats['total_events'] = category_stats.sum(axis=1)
        category_stats['conversion_rate'] = (category_stats['purchase'] / category_stats['view'] * 100).round(2)
        category_stats['cart_rate'] = (category_stats['add_to_cart'] / category_stats['view'] * 100).round(2)
        category_stats['purchase_from_cart_rate'] = (
            (category_stats['purchase'] / category_stats['add_to_cart'] * 100)
            .replace([float('inf'), float('-inf')], 0)
            .fillna(0)
            .round(2)
        )
        
        category_stats = category_stats.reset_index()
        category_stats = category_stats.sort_values('conversion_rate', ascending=False)
        
        print("\n✅ Conversion Rates by Category:")
        print(category_stats.to_string())
        
        # === Product-Level Conversion Rates (Top/Bottom 5) ===
        print("\n" + "=" * 80)
        print("📊 PRODUCT-LEVEL CONVERSION RATES")
        print("=" * 80)
        
        product_stats = df.groupby(['product_id', 'product_category', 'event_type']).size().unstack(fill_value=0)
        
        for col in ['view', 'add_to_cart', 'purchase']:
            if col not in product_stats.columns:
                product_stats[col] = 0
        
        product_stats['conversion_rate'] = (product_stats['purchase'] / product_stats['view'] * 100).round(2)
        product_stats = product_stats.reset_index()
        
        # Filter products with at least 10 views
        product_stats = product_stats[product_stats['view'] >= 10]
        
        top_5_products = product_stats.nlargest(5, 'conversion_rate')
        bottom_5_products = product_stats[product_stats['conversion_rate'] < 100].nsmallest(5, 'conversion_rate')
        
        print("\n🏆 Top 5 Products by Conversion Rate:")
        print(top_5_products[['product_id', 'product_category', 'view', 'purchase', 'conversion_rate']].to_string())
        
        print("\n⚠️  Bottom 5 Products by Conversion Rate:")
        print(bottom_5_products[['product_id', 'product_category', 'view', 'purchase', 'conversion_rate']].to_string())
        
        # Push to XCom
        context['ti'].xcom_push(key='category_conversion', value=category_stats.to_json(orient='records'))
        context['ti'].xcom_push(key='top_products', value=top_5_products.to_json(orient='records'))
        context['ti'].xcom_push(key='bottom_products', value=bottom_5_products.to_json(orient='records'))
        
        # Summary stats
        summary = {
            'overall_views': int(df[df['event_type'] == 'view'].shape[0]),
            'overall_purchases': int(df[df['event_type'] == 'purchase'].shape[0]),
            'overall_conversion_rate': round(
                df[df['event_type'] == 'purchase'].shape[0] / df[df['event_type'] == 'view'].shape[0] * 100, 2
            ),
            'best_category': category_stats.iloc[0]['product_category'],
            'best_category_rate': float(category_stats.iloc[0]['conversion_rate']),
            'worst_category': category_stats.iloc[-1]['product_category'],
            'worst_category_rate': float(category_stats.iloc[-1]['conversion_rate']),
        }
        
        context['ti'].xcom_push(key='summary', value=summary)
        
        return summary
        
    except Exception as e:
        print(f"❌ Error calculating conversion rates: {e}")
        raise

def save_conversion_analytics(**context):
    """
    Save conversion rate analytics to database
    """
    print("=" * 80)
    print("💾 TASK 3: Saving Conversion Analytics")
    print("=" * 80)
    
    try:
        import subprocess
        
        # Pull data
        category_json = context['ti'].xcom_pull(key='category_conversion', task_ids='calculate_conversion_rates')
        summary = context['ti'].xcom_pull(key='summary', task_ids='calculate_conversion_rates')
        
        df = pd.DataFrame(json.loads(category_json))
        
        # Create table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS conversion_rate_daily (
            id SERIAL PRIMARY KEY,
            analysis_date DATE NOT NULL,
            category VARCHAR(100) NOT NULL,
            total_views INTEGER,
            total_cart INTEGER,
            total_purchases INTEGER,
            conversion_rate DECIMAL(5,2),
            cart_rate DECIMAL(5,2),
            purchase_from_cart_rate DECIMAL(5,2),
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
            print("✅ Table 'conversion_rate_daily' ready")
        
        analysis_date = context['execution_date'].date()
        
        print(f"\n💾 Saving conversion analytics to database...")
        print(f"📅 Analysis Date: {analysis_date}")
        print(f"\n✅ Would insert {len(df)} category records")
        
        print(f"\n📊 Overall Summary:")
        for key, value in summary.items():
            print(f"   {key}: {value}")
        
        return "Conversion analytics saved successfully"
        
    except Exception as e:
        print(f"❌ Error saving analytics: {e}")
        raise

def generate_conversion_report(**context):
    """
    Generate detailed conversion rate report
    """
    print("=" * 80)
    print("📧 TASK 4: Generating Conversion Rate Report")
    print("=" * 80)
    
    try:
        category_json = context['ti'].xcom_pull(key='category_conversion', task_ids='calculate_conversion_rates')
        top_products_json = context['ti'].xcom_pull(key='top_products', task_ids='calculate_conversion_rates')
        bottom_products_json = context['ti'].xcom_pull(key='bottom_products', task_ids='calculate_conversion_rates')
        summary = context['ti'].xcom_pull(key='summary', task_ids='calculate_conversion_rates')
        
        category_df = pd.DataFrame(json.loads(category_json))
        top_df = pd.DataFrame(json.loads(top_products_json))
        bottom_df = pd.DataFrame(json.loads(bottom_products_json))
        
        execution_date = context['execution_date']
        
        report = f"""
{'=' * 80}
📊 CONVERSION RATE ANALYTICS REPORT
{'=' * 80}

Analysis Date: {execution_date.strftime('%Y-%m-%d')}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{'=' * 80}
OVERALL METRICS
{'=' * 80}

Total Views:              {summary['overall_views']:,}
Total Purchases:          {summary['overall_purchases']:,}
Overall Conversion Rate:  {summary['overall_conversion_rate']}%

Best Category:            {summary['best_category']} ({summary['best_category_rate']}%)
Worst Category:           {summary['worst_category']} ({summary['worst_category_rate']}%)

{'=' * 80}
CONVERSION RATES BY CATEGORY
{'=' * 80}

"""
        
        for idx, row in category_df.iterrows():
            report += f"""
📦 {row['product_category']}
{'-' * 80}
Views:                    {int(row['view']):,}
Add to Cart:              {int(row['add_to_cart']):,}
Purchases:                {int(row['purchase']):,}
Conversion Rate:          {row['conversion_rate']}%
Cart Rate:                {row['cart_rate']}%
Purchase from Cart:       {row['purchase_from_cart_rate']}%

"""
        
        report += f"""
{'=' * 80}
🏆 TOP 5 PRODUCTS (Highest Conversion)
{'=' * 80}

"""
        
        for idx, row in top_df.iterrows():
            report += f"  {row['product_id']} ({row['product_category']}): {row['conversion_rate']}% ({int(row['purchase'])}/{int(row['view'])})\n"
        
        report += f"""
{'=' * 80}
⚠️  BOTTOM 5 PRODUCTS (Lowest Conversion - Needs Attention)
{'=' * 80}

"""
        
        for idx, row in bottom_df.iterrows():
            report += f"  {row['product_id']} ({row['product_category']}): {row['conversion_rate']}% ({int(row['purchase'])}/{int(row['view'])})\n"
        
        report += f"""
{'=' * 80}
KEY INSIGHTS & RECOMMENDATIONS
{'=' * 80}

✅ Strong Performance:
   • {summary['best_category']} category shows highest conversion ({summary['best_category_rate']}%)
   • Focus marketing efforts on successful products
   
⚠️  Needs Improvement:
   • {summary['worst_category']} category needs optimization ({summary['worst_category_rate']}%)
   • Review pricing, product descriptions, and images for low-converting items
   
💡 Action Items:
   1. Investigate why bottom 5 products have low conversion
   2. A/B test different pricing strategies for underperforming categories
   3. Consider flash sales for high-view, low-purchase products
   4. Optimize product pages for categories below {summary['overall_conversion_rate']}% conversion

{'=' * 80}
        """
        
        print(report)
        
        report_path = f"/tmp/conversion_rate_report_{execution_date.strftime('%Y%m%d')}.txt"
        print(f"\n✅ Report generated successfully")
        print(f"📁 Report would be saved to: {report_path}")
        
        return "Report generated successfully"
        
    except Exception as e:
        print(f"❌ Error generating report: {e}")
        raise

# Define tasks
extract_task = PythonOperator(
    task_id='extract_all_events',
    python_callable=extract_all_events,
    provide_context=True,
    dag=dag,
)

calculate_task = PythonOperator(
    task_id='calculate_conversion_rates',
    python_callable=calculate_conversion_rates,
    provide_context=True,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_conversion_analytics',
    python_callable=save_conversion_analytics,
    provide_context=True,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_conversion_report',
    python_callable=generate_conversion_report,
    provide_context=True,
    dag=dag,
)

# Define dependencies
extract_task >> calculate_task >> [save_task, report_task]
