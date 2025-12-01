@echo off
REM Script to verify Airflow DAGs execution and check database results

echo ======================================================================
echo 🔍 Airflow DAG Verification Script
echo ======================================================================
echo.

:menu
echo ======================================================================
echo Verification Options:
echo ======================================================================
echo.
echo [1] Check if DAGs are loaded in Airflow
echo [2] View User Segmentation Results
echo [3] View Top Products Results
echo [4] View Conversion Rate Results
echo [5] Check All Database Tables
echo [6] View Recent DAG Logs
echo [7] Full Health Check
echo [8] Exit
echo.
set /p choice="Enter your choice (1-8): "

if "%choice%"=="1" goto check_dags
if "%choice%"=="2" goto verify_segmentation
if "%choice%"=="3" goto verify_products
if "%choice%"=="4" goto verify_conversion
if "%choice%"=="5" goto check_all_tables
if "%choice%"=="6" goto view_logs
if "%choice%"=="7" goto health_check
if "%choice%"=="8" goto end
echo Invalid choice! Please try again.
goto menu

:check_dags
echo.
echo ======================================================================
echo 📋 Checking Loaded DAGs
echo ======================================================================
docker exec airflow-scheduler airflow dags list | findstr /C:"daily_user_segmentation" /C:"top_products_daily_report" /C:"conversion_rate_analytics"
if %ERRORLEVEL%==0 (
    echo.
    echo ✅ All required DAGs are loaded
) else (
    echo.
    echo ❌ Some DAGs are missing!
    echo 💡 Check dags/ folder and Airflow logs
)
echo.
pause
goto menu

:verify_segmentation
echo.
echo ======================================================================
echo 👥 User Segmentation Results
echo ======================================================================
docker exec -it postgres psql -U airflow -d airflow -c "SELECT analysis_date, segment, COUNT(*) as user_count FROM user_segmentation_daily GROUP BY analysis_date, segment ORDER BY analysis_date DESC LIMIT 10;"
echo.
echo ======================================================================
echo 📊 Recent Segmentation Summary
echo ======================================================================
docker exec -it postgres psql -U airflow -d airflow -c "SELECT analysis_date, segment, COUNT(*) as users, ROUND(AVG(total_events), 2) as avg_events FROM user_segmentation_daily GROUP BY analysis_date, segment ORDER BY analysis_date DESC;"
echo.
pause
goto menu

:verify_products
echo.
echo ======================================================================
echo 🏆 Top Products Results
echo ======================================================================
docker exec -it postgres psql -U airflow -d airflow -c "SELECT analysis_date, rank, product_id, category, total_views, unique_viewers FROM top_products_daily ORDER BY analysis_date DESC, rank ASC LIMIT 10;"
echo.
pause
goto menu

:verify_conversion
echo.
echo ======================================================================
echo 📊 Conversion Rate Results
echo ======================================================================
docker exec -it postgres psql -U airflow -d airflow -c "SELECT analysis_date, category, total_views, total_purchases, conversion_rate FROM conversion_rate_daily ORDER BY analysis_date DESC, conversion_rate DESC;"
echo.
pause
goto menu

:check_all_tables
echo.
echo ======================================================================
echo 🗄️  Checking All Analytics Tables
echo ======================================================================
echo.
echo [1/4] Checking user_segmentation_daily table...
docker exec -it postgres psql -U airflow -d airflow -c "SELECT COUNT(*) as row_count FROM user_segmentation_daily;" 2>nul || echo ⚠️  Table not found
echo.
echo [2/4] Checking top_products_daily table...
docker exec -it postgres psql -U airflow -d airflow -c "SELECT COUNT(*) as row_count FROM top_products_daily;" 2>nul || echo ⚠️  Table not found
echo.
echo [3/4] Checking conversion_rate_daily table...
docker exec -it postgres psql -U airflow -d airflow -c "SELECT COUNT(*) as row_count FROM conversion_rate_daily;" 2>nul || echo ⚠️  Table not found
echo.
echo [4/4] Checking flash_sale_alerts table (from real-time pipeline)...
docker exec -it postgres psql -U airflow -d airflow -c "SELECT COUNT(*) as row_count FROM flash_sale_alerts;" 2>nul || echo ⚠️  Table not found
echo.
pause
goto menu

:view_logs
echo.
echo ======================================================================
echo 📜 Recent DAG Execution Logs
echo ======================================================================
echo.
echo User Segmentation DAG - Last Run:
docker exec airflow-scheduler airflow dags list-runs -d daily_user_segmentation --no-backfill --output table --limit 1 2>nul
echo.
echo Top Products Report DAG - Last Run:
docker exec airflow-scheduler airflow dags list-runs -d top_products_daily_report --no-backfill --output table --limit 1 2>nul
echo.
echo Conversion Rate Analytics DAG - Last Run:
docker exec airflow-scheduler airflow dags list-runs -d conversion_rate_analytics --no-backfill --output table --limit 1 2>nul
echo.
pause
goto menu

:health_check
echo.
echo ======================================================================
echo 🏥 Full Health Check
echo ======================================================================
echo.

echo [1/5] Checking Docker Containers...
docker ps --format "table {{.Names}}\t{{.Status}}" | findstr /C:"airflow" /C:"postgres"
echo.

echo [2/5] Checking DAG Loading...
docker exec airflow-scheduler airflow dags list | findstr /C:"daily_user_segmentation" /C:"top_products_daily_report" /C:"conversion_rate_analytics"
if %ERRORLEVEL%==0 (
    echo ✅ All DAGs loaded
) else (
    echo ❌ DAGs not loaded properly
)
echo.

echo [3/5] Checking Database Tables...
docker exec postgres psql -U airflow -d airflow -c "\dt" | findstr /C:"user_segmentation_daily" /C:"top_products_daily" /C:"conversion_rate_daily" /C:"flash_sale_alerts"
echo.

echo [4/5] Checking Recent DAG Runs...
docker exec airflow-scheduler airflow dags list-runs --no-backfill --output table --limit 5 2>nul
echo.

echo [5/5] Checking Airflow Web UI...
echo 💡 Access Airflow UI at: http://localhost:8087
echo    Username: admin
echo    Password: admin
echo.

echo ======================================================================
echo Health Check Complete!
echo ======================================================================
echo.
pause
goto menu

:end
echo.
echo ======================================================================
echo 👋 Exiting...
echo ======================================================================
echo.
