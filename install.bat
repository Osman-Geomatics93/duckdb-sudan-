@echo off
echo ============================================
echo   DuckDB Sudan Extension - Installer
echo ============================================
echo.

echo [1/3] Cloning repository...
git clone --recurse-submodules https://github.com/Osman-Geomatics93/duckdb-sudan-.git
if errorlevel 1 (
    echo ERROR: Git clone failed. Make sure Git is installed.
    pause
    exit /b 1
)

cd duckdb-sudan-

echo.
echo [2/3] Building extension (this may take several minutes)...
call build_release.bat
if errorlevel 1 (
    echo ERROR: Build failed.
    pause
    exit /b 1
)

echo.
echo ============================================
echo   Build complete!
echo ============================================
echo.
echo [3/3] Launching DuckDB...
echo.
echo   Try these commands inside DuckDB:
echo     LOAD sudan;
echo     SELECT * FROM SUDAN_Providers();
echo.
echo ============================================
echo.

build\release\duckdb.exe -unsigned
