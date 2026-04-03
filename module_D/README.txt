1. Подготовка:
   - Выполнить audit_tables.sql в DWH
   psql -h localhost -p 5432 -U user -d db -f audit_tables.sql

2. Мониторинг:
   - Запустить: python monitoring_setup.py
   - Смотреть метрики: http://localhost:8501/metrics

3. Проверки работают автоматически:
   - Streaming: при каждом событии в модуле Б
   - Batch: после каждой загрузки в модуле Г

4. Посмотреть результаты аудита:
   python3 monitor.py
   psql -h localhost -p 5432 -U user -d db
   SELECT * FROM data_quality_log ORDER BY timestamp DESC LIMIT 50;

