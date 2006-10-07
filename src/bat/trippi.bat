@echo off
if "%TRIPPI_HOME%" == "" set TRIPPI_HOME="."
if not exist "%TRIPPI_HOME%\trippi.bat" goto appNotFound
java -Xms64m -Xmx96m -Djava.endorsed.dirs="%TRIPPI_HOME%\lib" -Dtrippi.home="%TRIPPI_HOME%" org.trippi.ui.TrippiUI %1 %2 %3 %4 %5 %6 %7 %8 %9
goto end
:appNotFound
echo ERROR: When running trippi from a directory other than where it was 
echo        installed, you must define the TRIPPI_HOME environment variable
echo        to be the directory where it was installed.
:end
