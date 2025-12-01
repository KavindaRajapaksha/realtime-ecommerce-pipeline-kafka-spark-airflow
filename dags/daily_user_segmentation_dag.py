"""
Airflow DAG: Daily User Segmentation
Categorizes users into Window Shoppers vs Buyers
Runs daily at 1 AM to analyze previous day's data
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    'daily_user_segmentation',
    default_args=default_args,
    description='Daily user segmentation: Window Shoppers vs Buyers',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    catchup=False,
    tags=['analytics', 'batch', 'user-segmentation']
)

def extract_raw_events(**context):
    """
    Extract raw clickstream events from PostgreSQL
    In real scenario, this would query from data warehouse/data lake
    """
    print("=" * 80)
    print("📊 TASK 1: Extracting Raw Clickstream Events")
    print("=" * 80)
    
    try:
        # Create sample data since we don't have a raw events table yet
        # In production, you'd query: SELECT * FROM raw_clickstream WHERE date = YESTERDAY
        
        # For demo, we'll create sample data based on your producer pattern
        import random
        
        events = []
        execution_date = context['execution_date']
        
        # Generate sample events (simulating yesterday's data)
        user_ids = [f"user_{i:04d}" for i in range(1, 101)]
        product_ids = [f"prod_{i:04d}" for i in range(1, 51)]
        event_types = ['view', 'add_to_cart', 'purchase']
        
        for _ in range(1000):  # 1000 events
            user_id = random.choice(user_ids)
            event = {
                'user_id': user_id,
                'product_id': random.choice(product_ids),
                'event_type': random.choices(event_types, weights=[0.70, 0.20, 0.10])[0],
                'timestamp': (execution_date - timedelta(days=1)).isoformat(),
            }
            events.append(event)
        
        # Convert to DataFrame
        df = pd.DataFrame(events)
        
        print(f"\n✅ Extracted {len(df)} events")
        print(f"📅 Date: {execution_date.date()}")
        print(f"\nEvent Type Distribution:")
        print(df['event_type'].value_counts())
        
        # Push to XCom for next task
        context['ti'].xcom_push(key='raw_events', value=df.to_json(orient='records'))
        
        return f"Extracted {len(df)} events"
        
    except Exception as e:
        print(f"❌ Error extracting data: {e}")
        raise

def segment_users(**context):
    """
    Segment users into Window Shoppers (only view/add_to_cart) vs Buyers (made purchase)
    """
    print("=" * 80)
    print("🔍 TASK 2: Segmenting Users")
    print("=" * 80)
    
    try:
        # Pull data from previous task
        raw_events_json = context['ti'].xcom_pull(key='raw_events', task_ids='extract_raw_events')
        df = pd.DataFrame(json.loads(raw_events_json))
        
        # Segment users
        user_segments = df.groupby('user_id')['event_type'].apply(list).reset_index()
        
        def classify_user(events):
            if 'purchase' in events:
                return 'Buyer'
            elif 'add_to_cart' in events or 'view' in events:
                return 'Window Shopper'
            else:
                return 'Unknown'
        
        user_segments['segment'] = user_segments['event_type'].apply(classify_user)
        user_segments['total_events'] = user_segments['event_type'].apply(len)
        user_segments['view_count'] = user_segments['event_type'].apply(lambda x: x.count('view'))
        user_segments['cart_count'] = user_segments['event_type'].apply(lambda x: x.count('add_to_cart'))
        user_segments['purchase_count'] = user_segments['event_type'].apply(lambda x: x.count('purchase'))
        
        # Drop the raw event_type list
        user_segments = user_segments.drop(columns=['event_type'])
        
        print(f"\n✅ Segmented {len(user_segments)} users")
        print(f"\nSegmentation Results:")
        print(user_segments['segment'].value_counts())
        
        print(f"\n📊 Sample Segments:")
        print(user_segments.head(10).to_string())
        
        # Push to XCom
        context['ti'].xcom_push(key='user_segments', value=user_segments.to_json(orient='records'))
        
        # Also push summary stats
        summary = {
            'total_users': len(user_segments),
            'buyers': len(user_segments[user_segments['segment'] == 'Buyer']),
            'window_shoppers': len(user_segments[user_segments['segment'] == 'Window Shopper']),
            'buyer_percentage': round(len(user_segments[user_segments['segment'] == 'Buyer']) / len(user_segments) * 100, 2),
            'execution_date': context['execution_date'].isoformat()
        }
        
        context['ti'].xcom_push(key='summary_stats', value=summary)
        
        return summary
        
    except Exception as e:
        print(f"❌ Error segmenting users: {e}")
        raise

def save_segmentation_results(**context):
    """
    Save segmentation results to PostgreSQL for historical analysis
    """
    print("=" * 80)
    print("💾 TASK 3: Saving Segmentation Results")
    print("=" * 80)
    
    try:
        # Pull data from previous task
        user_segments_json = context['ti'].xcom_pull(key='user_segments', task_ids='segment_users')
        summary_stats = context['ti'].xcom_pull(key='summary_stats', task_ids='segment_users')
        
        df = pd.DataFrame(json.loads(user_segments_json))
        
        # Create table if not exists using PostgresHook
        from airflow.providers.postgres.operators.postgres import PostgresOperator
        import subprocess
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS user_segmentation_daily (
            id SERIAL PRIMARY KEY,
            analysis_date DATE NOT NULL,
            user_id VARCHAR(50) NOT NULL,
            segment VARCHAR(50) NOT NULL,
            total_events INTEGER,
            view_count INTEGER,
            cart_count INTEGER,
            purchase_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Execute via docker
        cmd = [
            "docker", "exec", "-i", "postgres",
            "psql", "-U", "airflow", "-d", "airflow",
            "-c", create_table_sql
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 or "already exists" in result.stderr:
            print("✅ Table 'user_segmentation_daily' ready")
        
        # Insert data (in production, use PostgresHook.insert_rows for efficiency)
        analysis_date = context['execution_date'].date()
        
        print(f"\n💾 Saving {len(df)} user segments to database...")
        print(f"📅 Analysis Date: {analysis_date}")
        
        # For demo, we'll print what would be inserted
        print(f"\n✅ Would insert {len(df)} rows into 'user_segmentation_daily'")
        print(f"\n📊 Summary Statistics:")
        for key, value in summary_stats.items():
            print(f"   {key}: {value}")
        
        return summary_stats
        
    except Exception as e:
        print(f"❌ Error saving results: {e}")
        raise

def generate_email_report(**context):
    """
    Generate formatted text report suitable for email
    """
    print("=" * 80)
    print("📧 TASK 4: Generating Email Report")
    print("=" * 80)
    
    try:
        summary_stats = context['ti'].xcom_pull(key='summary_stats', task_ids='segment_users')
        
        report = f"""
{'=' * 80}
📊 DAILY USER SEGMENTATION REPORT
{'=' * 80}

Analysis Date: {summary_stats['execution_date'][:10]}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{'=' * 80}
SUMMARY STATISTICS
{'=' * 80}

Total Users Analyzed:        {summary_stats['total_users']}
Buyers (Made Purchase):      {summary_stats['buyers']} ({summary_stats['buyer_percentage']}%)
Window Shoppers (No Buy):    {summary_stats['window_shoppers']} ({100 - summary_stats['buyer_percentage']:.2f}%)

{'=' * 80}
KEY INSIGHTS
{'=' * 80}

✅ Buyer Conversion Rate: {summary_stats['buyer_percentage']}%
⚠️  {summary_stats['window_shoppers']} users browsed but didn't purchase
💡 Recommendation: Target window shoppers with personalized offers

{'=' * 80}
NEXT STEPS
{'=' * 80}

1. Send retargeting emails to window shoppers
2. Analyze which products had high views but low conversions
3. Consider flash sales for high-interest, low-purchase products

{'=' * 80}
        """
        
        print(report)
        
        # Save report to file
        report_path = f"/tmp/user_segmentation_report_{context['execution_date'].strftime('%Y%m%d')}.txt"
        
        print(f"\n✅ Report generated successfully")
        print(f"📁 Report would be saved to: {report_path}")
        
        return "Report generated successfully"
        
    except Exception as e:
        print(f"❌ Error generating report: {e}")
        raise

# Define tasks
extract_task = PythonOperator(
    task_id='extract_raw_events',
    python_callable=extract_raw_events,
    provide_context=True,
    dag=dag,
)

segment_task = PythonOperator(
    task_id='segment_users',
    python_callable=segment_users,
    provide_context=True,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_segmentation_results',
    python_callable=save_segmentation_results,
    provide_context=True,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_email_report',
    python_callable=generate_email_report,
    provide_context=True,
    dag=dag,
)

# Define dependencies
extract_task >> segment_task >> [save_task, report_task]
