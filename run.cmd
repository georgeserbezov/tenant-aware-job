@echo off
rem Windows equivalent of run.sh: builds the React UI into the Spring Boot app
rem and starts everything on one port. See run.sh for the reasoning.
setlocal
cd /d "%~dp0"

set "STATIC_DIR=src\main\resources\static"

rem 1. Resolve a JDK 21+
set "JAVA_EXE=java"
if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" set "JAVA_EXE=%JAVA_HOME%\bin\java.exe"

"%JAVA_EXE%" -version >nul 2>&1
if errorlevel 1 goto :nojdk

for /f "tokens=3" %%v in ('"%JAVA_EXE%" -version 2^>^&1 ^| findstr /i "version"') do set "JAVA_VER=%%~v"
if not defined JAVA_VER goto :nojdk
for /f "delims=." %%m in ("%JAVA_VER%") do set "JAVA_MAJOR=%%m"
if not defined JAVA_MAJOR goto :nojdk
if %JAVA_MAJOR% LSS 21 goto :oldjdk
echo == Using Java %JAVA_VER%

rem 2. Build the React UI into the Spring static directory
if not exist ui goto :runserver
where npm >nul 2>&1
if errorlevel 1 goto :nonpm

echo == Building React UI
pushd ui
if exist package-lock.json (
  call npm ci --silent
) else (
  call npm install --silent
)
if errorlevel 1 (popd & goto :uifail)
call npm run build --silent
if errorlevel 1 (popd & goto :uifail)
popd

if exist "%STATIC_DIR%" rmdir /s /q "%STATIC_DIR%"
mkdir "%STATIC_DIR%"
xcopy /e /i /q /y "ui\dist\*" "%STATIC_DIR%\" >nul
if errorlevel 1 goto :uifail
echo == UI built into %STATIC_DIR%

:runserver
rem 3. Run the server. Any arguments are forwarded as Spring properties.
echo == Starting on http://localhost:8080
if "%~1"=="" (
  call mvnw.cmd -q spring-boot:run
) else (
  call mvnw.cmd -q spring-boot:run -Dspring-boot.run.arguments="%*"
)
exit /b %ERRORLEVEL%

:nojdk
echo ERROR: No JDK 21+ found on PATH or at JAVA_HOME.
echo   Install one from https://adoptium.net/temurin/releases/?version=21
echo   or set JAVA_HOME to an existing JDK 21+ installation.
exit /b 1

:oldjdk
echo ERROR: Java %JAVA_VER% found, but this project needs 21 or newer.
echo   Set JAVA_HOME to a JDK 21+ installation and re-run.
exit /b 1

:nonpm
echo ERROR: npm is required to build the UI. Install Node.js from https://nodejs.org
exit /b 1

:uifail
echo ERROR: the React UI failed to build.
exit /b 1
