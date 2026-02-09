@echo off
call "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64

set PATH=%PATH%;C:\Users\wwwos\AppData\Roaming\Python\Python39\Scripts
set PROJ_DIR=D:\Udemy_Cour\DuckDB\duckdb-sudan
set VCPKG_TOOLCHAIN=C:\vcpkg\scripts\buildsystems\vcpkg.cmake

cd /d "%PROJ_DIR%"

if not exist "build\release" mkdir build\release

cmake -G Ninja -DEXTENSION_STATIC_BUILD=1 -DDUCKDB_EXTENSION_CONFIGS="%PROJ_DIR%\extension_config.cmake" -DCMAKE_BUILD_TYPE=Release -DENABLE_UNITTEST_CPP_TESTS=FALSE -DVCPKG_BUILD=1 -DCMAKE_TOOLCHAIN_FILE="%VCPKG_TOOLCHAIN%" -DVCPKG_MANIFEST_DIR="%PROJ_DIR%" -DVCPKG_OVERLAY_PORTS="%PROJ_DIR%\extension-ci-tools\vcpkg_ports" -S duckdb -B build\release

if %ERRORLEVEL% NEQ 0 (
    echo CMAKE CONFIGURE FAILED
    exit /b %ERRORLEVEL%
)

cmake --build build\release --config Release

if %ERRORLEVEL% NEQ 0 (
    echo CMAKE BUILD FAILED
    exit /b %ERRORLEVEL%
)

echo BUILD SUCCEEDED
