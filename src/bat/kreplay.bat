@echo off
if "%KREPLAY_HOME%" == "" set KREPLAY_HOME="."
java -Xmn32m -Xms128m -Xmx128m -Dkreplay.home="%KREPLAY_HOME%" -Djava.endorsed.dirs="%KREPLAY_HOME%\lib" -jar "%KREPLAY_HOME%\kreplay.jar" %1 %2 %3 %4 %5 %6 %7 %8 %9
