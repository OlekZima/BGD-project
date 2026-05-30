@echo off
setlocal

if not exist .env (
    powershell -NoProfile -Command "$p = Read-Host 'Enter PostgreSQL password'; Set-Content -Path .env -Value \"POSTGRES_PASSWORD=$p\" -Encoding ascii"
    echo Password saved to .env
)

docker compose up -d
if %errorlevel% neq 0 (
    echo.
    echo First attempt failed - releasing stale port binding and retrying...
    docker compose down
    docker compose up -d
    if %errorlevel% neq 0 (
        echo.
        echo ERROR: docker compose failed - see above.
        pause
        exit /b 1
    )
)

echo.
echo PostgreSQL is starting - first run initialises the schema in a few seconds.
echo Monitor:  docker logs -f bgd_postgres
echo Connect:  psql postgresql://citibike:^<password^>@localhost:5432/citibike
echo.
pause
