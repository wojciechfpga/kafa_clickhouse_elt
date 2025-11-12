@echo off
docker-compose up --build -d
timeout /t 50 /nobreak
python data_generator.py
