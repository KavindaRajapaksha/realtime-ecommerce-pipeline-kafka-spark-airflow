@echo off
REM Script to comprehensively check if Airflow DAGs are working correctly

echo ======================================================================
echo 🔍 AIRFLOW DAG STATUS CHECKER
echo ======================================================================
echo.

echo [1/6] Checking if DAGs are loaded...
echo ----------------------------------------------------------------------
docker exec airflow-scheduler airflow dags list 2>nul | findstr /C:"daily_user_segmentation" /C:"top_products_daily_report" /C:"conversion_rate_analytics"
if %ERRORLEVEL%==0 (
    echo ✅ All 3 DAGs are loaded in Airflow
    set DAGS_LOADED=1
) else (
    echo ❌ DAGs are NOT loaded properly
    set DAGS_LOADED=0
    goto :failed
)
echo.

echo [2/6] Checking DAG parsing errors...
echo ----------------------------------------------------------------------
docker exec airflow-scheduler airflow dags list-import-errors 2>nul > temp_errors.txt
findstr /C:"daily_user_segmentation" /C:"top_products_daily_report" /C:"conversion_rate_analytics" temp_errors.txt >nul 2>&1
if %ERRORLEVEL%==0 (
    echo ❌ DAGs have parsing errors:
    type temp_errors.txt
    del temp_errors.txt
    set DAGS_PARSED=0
    goto :failed
) else (
    echo ✅ No parsing errors - all DAGs are valid Python code
    del temp_errors.txt
    set DAGS_PARSED=1
)
echo.

echo [3/6] Checking if DAGs have been triggered/executed...
echo ----------------------------------------------------------------------

REM Check all three DAGs for any runs
set DAGS_RUN=0
docker exec airflow-scheduler airflow dags list-runs -d daily_user_segmentation --no-backfill > temp_runs.txt 2>&1
findstr /C:"daily_user_segmentation" temp_runs.txt >nul 2>&1
if %ERRORLEVEL%==0 set DAGS_RUN=1

docker exec airflow-scheduler airflow dags list-runs -d top_products_daily_report --no-backfill >> temp_runs.txt 2>&1
findstr /C:"top_products_daily_report" temp_runs.txt >nul 2>&1
if %ERRORLEVEL%==0 set DAGS_RUN=1

docker exec airflow-scheduler airflow dags list-runs -d conversion_rate_analytics --no-backfill >> temp_runs.txt 2>&1
findstr /C:"conversion_rate_analytics" temp_runs.txt >nul 2>&1
if %ERRORLEVEL%==0 set DAGS_RUN=1

if %DAGS_RUN%==1 (
    echo ✅ DAGs have been executed at least once
) else (
    echo ⚠️  DAGs have NOT been triggered yet
    echo 💡 Run: trigger_airflow_dags.bat to execute them
    del temp_runs.txt 2>nul
    goto :summary
)

REM Check run states
findstr /C:"success" temp_runs.txt >nul 2>&1
if %ERRORLEVEL%==0 (
    echo ✅ At least one DAG run completed successfully
    set DAGS_SUCCESS=1
) else (
    set DAGS_SUCCESS=0
)

findstr /C:"failed" temp_runs.txt >nul 2>&1
if %ERRORLEVEL%==0 (
    echo ❌ Some DAG runs have FAILED
    set DAGS_FAILED=1
) else (
    set DAGS_FAILED=0
)

findstr /C:"running" temp_runs.txt >nul 2>&1
if %ERRORLEVEL%==0 (
    echo ⏳ Some DAGs are currently RUNNING
    set DAGS_RUNNING=1
) else (
    set DAGS_RUNNING=0
)

del temp_runs.txt
echo.

echo [4/6] Checking latest run status for each DAG...
echo ----------------------------------------------------------------------

REM Check User Segmentation DAG
echo 📊 User Segmentation DAG:
docker exec airflow-scheduler airflow dags list-runs -d daily_user_segmentation --no-backfill --state success 2>nul | findstr /C:"success" >nul 2>&1
if %ERRORLEVEL%==0 (
    echo    ✅ Has successful run
    set DAG1_OK=1
) else (
    docker exec airflow-scheduler airflow dags list-runs -d daily_user_segmentation --no-backfill --state failed 2>nul | findstr /C:"failed" >nul 2>&1
    if %ERRORLEVEL%==0 (
        echo    ❌ Latest run FAILED
        set DAG1_OK=0
    ) else (
        echo    ⏳ Running or not yet executed
        set DAG1_OK=0
    )
)

REM Check Top Products DAG
echo 📊 Top Products Report DAG:
docker exec airflow-scheduler airflow dags list-runs -d top_products_daily_report --no-backfill --state success 2>nul | findstr /C:"success" >nul 2>&1
if %ERRORLEVEL%==0 (
    echo    ✅ Has successful run
    set DAG2_OK=1
) else (
    docker exec airflow-scheduler airflow dags list-runs -d top_products_daily_report --no-backfill --state failed 2>nul | findstr /C:"failed" >nul 2>&1
    if %ERRORLEVEL%==0 (
        echo    ❌ Latest run FAILED
        set DAG2_OK=0
    ) else (
        echo    ⏳ Running or not yet executed
        set DAG2_OK=0
    )
)

REM Check Conversion Rate DAG
echo 📊 Conversion Rate Analytics DAG:
docker exec airflow-scheduler airflow dags list-runs -d conversion_rate_analytics --no-backfill --state success 2>nul | findstr /C:"success" >nul 2>&1
if %ERRORLEVEL%==0 (
    echo    ✅ Has successful run
    set DAG3_OK=1
) else (
    docker exec airflow-scheduler airflow dags list-runs -d conversion_rate_analytics --no-backfill --state failed 2>nul | findstr /C:"failed" >nul 2>&1
    if %ERRORLEVEL%==0 (
        echo    ❌ Latest run FAILED
        set DAG3_OK=0
    ) else (
        echo    ⏳ Running or not yet executed
        set DAG3_OK=0
    )
)
echo.

echo [5/6] Checking task execution details...
echo ----------------------------------------------------------------------
docker exec airflow-scheduler airflow dags list-runs --no-backfill --state success 2>nul | findstr /C:"daily_user_segmentation" /C:"top_products_daily_report" /C:"conversion_rate_analytics" >nul 2>&1
if %ERRORLEVEL%==0 (
    echo ✅ Successfully executed DAG runs found
    echo.
    echo Recent successful runs:
    docker exec airflow-scheduler airflow dags list-runs --no-backfill --state success --output table 2>nul | findstr /C:"daily_user_segmentation" /C:"top_products_daily_report" /C:"conversion_rate_analytics" /C:"dag_id"
) else (
    echo ⚠️  No successful DAG runs found yet
)
echo.

echo [6/6] Checking Airflow services health...
echo ----------------------------------------------------------------------
docker ps --format "table {{.Names}}\t{{.Status}}" | findstr /C:"airflow-scheduler" /C:"airflow-webserver"
echo.

:summary
echo ======================================================================
echo 📋 SUMMARY
echo ======================================================================
echo.

set /a TOTAL_CHECKS=0
set /a PASSED_CHECKS=0

if defined DAGS_LOADED (
    if %DAGS_LOADED%==1 (
        echo ✅ DAGs Loaded: YES
        set /a PASSED_CHECKS+=1
    ) else (
        echo ❌ DAGs Loaded: NO
    )
    set /a TOTAL_CHECKS+=1
)

if defined DAGS_PARSED (
    if %DAGS_PARSED%==1 (
        echo ✅ DAGs Parsed: YES - no errors
        set /a PASSED_CHECKS+=1
    ) else (
        echo ❌ DAGs Parsed: NO - has errors
    )
    set /a TOTAL_CHECKS+=1
)

if defined DAGS_RUN (
    if %DAGS_RUN%==1 (
        echo ✅ DAGs Executed: YES
        set /a PASSED_CHECKS+=1
        set /a TOTAL_CHECKS+=1
    ) else (
        echo ⚠️  DAGs Executed: NO - not triggered yet
        set /a TOTAL_CHECKS+=1
        goto :final_verdict
    )
)

if defined DAG1_OK (
    if %DAG1_OK%==1 (
        echo ✅ User Segmentation DAG: SUCCESS
        set /a PASSED_CHECKS+=1
    ) else (
        echo ❌ User Segmentation DAG: FAILED or NOT RUN
    )
    set /a TOTAL_CHECKS+=1
)

if defined DAG2_OK (
    if %DAG2_OK%==1 (
        echo ✅ Top Products Report DAG: SUCCESS
        set /a PASSED_CHECKS+=1
    ) else (
        echo ❌ Top Products Report DAG: FAILED or NOT RUN
    )
    set /a TOTAL_CHECKS+=1
)

if defined DAG3_OK (
    if %DAG3_OK%==1 (
        echo ✅ Conversion Rate Analytics DAG: SUCCESS
        set /a PASSED_CHECKS+=1
    ) else (
        echo ❌ Conversion Rate Analytics DAG: FAILED or NOT RUN
    )
    set /a TOTAL_CHECKS+=1
)

echo.
echo Score: %PASSED_CHECKS%/%TOTAL_CHECKS% checks passed
echo.

:final_verdict
echo ======================================================================
echo 🎯 FINAL VERDICT
echo ======================================================================
echo.

if not defined PASSED_CHECKS set PASSED_CHECKS=0
if not defined TOTAL_CHECKS set TOTAL_CHECKS=0

if "%PASSED_CHECKS%"=="%TOTAL_CHECKS%" (
    echo ✅ ✅ ✅ ALL DAGS ARE WORKING PERFECTLY! ✅ ✅ ✅
    echo.
    echo 🎉 Congratulations! Your Airflow orchestration is fully functional:
    echo    - All 3 DAGs loaded successfully
    echo    - No parsing errors
    echo    - All DAG runs completed successfully
    echo    - Batch analytics pipeline is operational
    echo.
    echo 📊 View detailed reports in Airflow UI: http://localhost:8087
    exit /b 0
)

if %PASSED_CHECKS% GEQ 3 (
    echo ⚠️  DAGS ARE MOSTLY WORKING BUT NEED ATTENTION
    echo.
    echo 💡 Next steps:
    if not defined DAGS_RUN set DAGS_RUN=0
    if "%DAGS_RUN%"=="0" (
        echo    1. Trigger the DAGs using: trigger_airflow_dags.bat
    )
    if defined DAGS_FAILED (
        if "%DAGS_FAILED%"=="1" (
            echo    2. Check failed task logs in Airflow UI: http://localhost:8087
            echo    3. Fix any errors and re-trigger the DAGs
        )
    )
    exit /b 1
)

goto :failed

:failed
echo ❌ ❌ ❌ DAGS ARE NOT WORKING PROPERLY ❌ ❌ ❌
echo.
echo 🔧 Troubleshooting steps:
echo    1. Check if Airflow containers are running: docker-compose ps
echo    2. Restart Airflow: docker-compose restart airflow-scheduler airflow-webserver
echo    3. Check scheduler logs: docker logs airflow-scheduler --tail 50
echo    4. Verify DAG files exist in dags/ folder
echo    5. Check for Python syntax errors in DAG files
echo    6. Try triggering DAGs: trigger_airflow_dags.bat
echo.
echo 📚 For more help, check: ORCHESTRATION_GUIDE.md
exit /b 1
