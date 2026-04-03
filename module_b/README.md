1. Потоковая обработка – реальное время или фрагмент видеонаблюдения: детекция транспортных средств, запись в Bronze слой, агрегация в Gold витрину
2. Пакетная обработка – историческая: трансформация Bronze -> Silver

Архитектура соответствует медальонной модели, определённой в Модуле А
- Bronze – сырые детекции (ClickHouse)
- Silver – очищенные данные с историей 
- Gold – агрегированные витрины для BI/ML

## Системные требования
- Python 3.8+
- Docker и Docker Compose
- 8+ GB RAM
- Видеофайл `fallback2.mp4` в корневой папке

## Компоненты системы
- YOLO - нейросетевая модель для детекции объектов
- pgAdmin - веб-интерфейс управления PostgreSQL
- Prometheus - сбор метрик (опционально)
- OpenCV - визуализация процесса детекции
- Clickhouse - БД для хранения данных трекинга

## Python зависимости
pip install ultralytics opencv-python numpy clickhouse-driver

## Запуск
Перейдите в необходимые папки ()
Заупстите скрипт - python camera.py ('q' остановить скрипт)

## Поверка таблиц из модуля А
 1. Последние 10 записей - sudo docker exec clickhouse clickhouse-client --query "
SELECT camera_id, ts, vehicle_type, speed, confidence 
FROM bronze.raw_detections 
ORDER BY ts DESC 
LIMIT 10
FORMAT Pretty
"
 2. sudo docker exec clickhouse clickhouse-client --query "
SELECT ts, segment_id, avg_speed, intensity, congestion_level 
FROM gold.mart_current_congestion 
ORDER BY ts DESC 
LIMIT 10
FORMAT Pretty
"
