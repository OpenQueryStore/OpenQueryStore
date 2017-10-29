@echo off

:loop

sqlcmd.exe -Slocalhost -dAdventureworks -E -i.\Demo\AW_Workload_random_waits.sql

goto loop