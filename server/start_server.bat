@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
cd /d C:\Users\MHKK\bbb_mass_extraction\server
start "BB5PipelineServer" /MIN "C:\Program Files\Python311\python.exe" -u pipeline_server.py
echo Server started.
