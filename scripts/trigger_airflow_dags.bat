@echo off
REM Script to trigger Airflow DAGs manually for testing

echo ======================================================================
echo 🚀 Airflow DAG Trigger Script
echo ======================================================================
echo.

echo Available DAGs:
echo   1. daily_user_segmentation
echo   2. top_products_daily_report
echo   3. conversion_rate_analytics
echo.

:menu
echo ======================================================================
echo What would you like to do?
echo ======================================================================
echo.
echo [1] Trigger All DAGs
echo [2] Trigger User Segmentation DAG
echo [3] Trigger Top Products Report DAG
echo [4] Trigger Conversion Rate Analytics DAG
echo [5] List All DAGs
echo [6] Check DAG Status
echo [7] Exit
echo.
set /p choice="Enter your choice (1-7): "

if "%choice%"=="1" goto trigger_all
if "%choice%"=="2" goto trigger_segmentation
if "%choice%"=="3" goto trigger_products
if "%choice%"=="4" goto trigger_conversion
if "%choice%"=="5" goto list_dags
if "%choice%"=="6" goto check_status
if "%choice%"=="7" goto end
echo Invalid choice! Please try again.
goto menu

:trigger_all
echo.
echo ======================================================================
echo 🚀 Triggering All DAGs
echo ======================================================================
echo.
echo [1/3] Triggering User Segmentation DAG...
docker exec airflow-scheduler airflow dags trigger daily_user_segmentation
timeout /t 2 >nul

echo [2/3] Triggering Top Products Report DAG...
docker exec airflow-scheduler airflow dags trigger top_products_daily_report
timeout /t 2 >nul

echo [3/3] Triggering Conversion Rate Analytics DAG...
docker exec airflow-scheduler airflow dags trigger conversion_rate_analytics
timeout /t 2 >nul

echo.
echo ✅ All DAGs triggered successfully!
echo ⏱️  Wait 30-60 seconds for execution
echo 💡 Use option [6] to check status
echo.
pause
goto menu

:trigger_segmentation
echo.
echo ======================================================================
echo 🚀 Triggering User Segmentation DAG
echo ======================================================================
docker exec airflow-scheduler airflow dags trigger daily_user_segmentation
echo.
echo ✅ DAG triggered successfully!
echo.
pause
goto menu

:trigger_products
echo.
echo ======================================================================
echo 🚀 Triggering Top Products Report DAG
echo ======================================================================
docker exec airflow-scheduler airflow dags trigger top_products_daily_report
echo.
echo ✅ DAG triggered successfully!
echo.
pause
goto menu

:trigger_conversion
echo.
echo ======================================================================
echo 🚀 Triggering Conversion Rate Analytics DAG
echo ======================================================================
docker exec airflow-scheduler airflow dags trigger conversion_rate_analytics
echo.
echo ✅ DAG triggered successfully!
echo.
pause
goto menu

:list_dags
echo.
echo ======================================================================
echo 📋 Listing All DAGs
echo ======================================================================
docker exec airflow-scheduler airflow dags list
echo.
pause
goto menu

:check_status
echo.
echo ======================================================================
echo 📊 Checking DAG Run Status
echo ======================================================================
echo.
echo User Segmentation DAG:
docker exec airflow-scheduler airflow dags list-runs -d daily_user_segmentation --no-backfill --output table 2>nul || echo No runs found
echo.
echo Top Products Report DAG:
docker exec airflow-scheduler airflow dags list-runs -d top_products_daily_report --no-backfill --output table 2>nul || echo No runs found
echo.
echo Conversion Rate Analytics DAG:
docker exec airflow-scheduler airflow dags list-runs -d conversion_rate_analytics --no-backfill --output table 2>nul || echo No runs found
echo.
pause
goto menu

:end
echo.
echo ======================================================================
echo 👋 Exiting...
echo ======================================================================
echo.
