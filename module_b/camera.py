from subprocess import Popen, PIPE
import numpy as np
from ultralytics import YOLO
import cv2
import os
import signal
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from clickhouse_driver import Client
from dq_checks import check_streaming
import psycopg2
from psycopg2.extras import DictCursor

ch_client = Client(
    host='localhost',
    port=9000,
    user='default',
    password=''
)

conn = psycopg2.connect(
            host='localhost',  
            port=5432,
            database='db',      
            user='user',
            password='0000'
        )

def create_tables_module_a():
    ch_client.execute("CREATE DATABASE IF NOT EXISTS bronze")
    ch_client.execute("CREATE DATABASE IF NOT EXISTS silver")
    ch_client.execute("CREATE DATABASE IF NOT EXISTS gold")
    ch_client.execute("CREATE DATABASE IF NOT EXISTS monitoring") 
    
    ch_client.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_detections (
            camera_id UInt32,
            ts DateTime64(3),
            vehicle_type String,
            speed Float32,
            confidence Float32,
            frame_path String
        ) ENGINE = MergeTree() ORDER BY ts
    """)
    
    ch_client.execute("""
        CREATE TABLE IF NOT EXISTS gold.mart_current_congestion (
            ts DateTime,
            segment_id UInt32,
            avg_speed Float32,
            intensity UInt32,
            congestion_level UInt8,
            update_time DateTime DEFAULT now()
        ) ENGINE = SummingMergeTree()
        ORDER BY (segment_id, ts)
        PARTITION BY toYYYYMM(ts)
    """)
    
    ch_client.execute("""
        CREATE TABLE IF NOT EXISTS monitoring.data_quality_log (
            id UInt64,
            pipeline String,
            check_name String,
            timestamp DateTime,
            passed UInt8,
            details String,
            camera_id UInt32,
            object_id String,
            severity UInt8 DEFAULT 1
        ) ENGINE = MergeTree() 
        ORDER BY (timestamp, pipeline)
        PARTITION BY toYYYYMM(timestamp)
    """)

create_tables_module_a()
log_id_counter = 0

aggregation_cache = defaultdict(lambda: {'speeds': [], 'count': 0})
last_flush_time = datetime.now()
duplicate_cache = set()  

def flush_aggregates_to_gold():
    global aggregation_cache
    if not aggregation_cache:
        return
    
    for (segment_id, minute_ts), data in aggregation_cache.items():
        if len(data['speeds']) == 0:
            continue
        avg_speed = sum(data['speeds']) / len(data['speeds'])
        intensity = data['count']
        
        if avg_speed >= 60: level = 0
        elif avg_speed >= 40: level = 1
        elif avg_speed >= 20: level = 2
        elif avg_speed >= 10: level = 3
        else: level = 4
        
        ch_client.execute("""
            INSERT INTO gold.mart_current_congestion 
            (ts, segment_id, avg_speed, intensity, congestion_level)
            VALUES (%(ts)s, %(segment_id)s, %(avg_speed)s, %(intensity)s, %(congestion_level)s)
        """, {
            'ts': minute_ts,
            'segment_id': segment_id,
            'avg_speed': avg_speed,
            'intensity': intensity,
            'congestion_level': level
        })
    aggregation_cache.clear()

STREAM_URL = "fallback2.mp4"
VIDEO_SIZE = (640, 360)

if not os.path.exists(STREAM_URL):
    print(f"Файл {STREAM_URL} не найден!")
    sys.exit(1)

model = YOLO("./yolo26n.pt")

ffmpeg_cmd = [
    "ffmpeg", "-v", "error", "-i", STREAM_URL,
    "-vf", f"fps=2,scale={VIDEO_SIZE[0]}:{VIDEO_SIZE[1]}",
    "-f", "rawvideo", "-pix_fmt", "rgb24", "-"
]

def signal_handler(sig, frame):
    print("\n Завершение работы...")
    flush_aggregates_to_gold()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

try:
    with Popen(ffmpeg_cmd, stdout=PIPE, stderr=PIPE) as proc:
        frame_count = 0
        frame_size = VIDEO_SIZE[0] * VIDEO_SIZE[1] * 3
        
        print("Начинаем обработку видео...")
        
        while True:
            buf = proc.stdout.read(frame_size)
            if len(buf) == 0:
                print("Конец видео")
                break
            if len(buf) != frame_size:
                continue
                
            frame = np.frombuffer(buf, np.uint8).reshape((VIDEO_SIZE[1], VIDEO_SIZE[0], 3))
            results = model.track(frame, persist=True, verbose=False)
            
            if results[0].boxes is not None:
                boxes = results[0].boxes
                track_ids = boxes.id.int().tolist() if boxes.id is not None else [None] * len(boxes)
                
                for box, track_id in zip(boxes, track_ids):
                    conf = box.conf[0].item()
                    cls_id = int(box.cls[0].item())
                    cls_name = model.names[cls_id]
                    
                    try:
                        ch_client.execute("""
                            INSERT INTO bronze.raw_detections 
                            (camera_id, ts, vehicle_type, speed, confidence, frame_path)
                            VALUES (%(camera_id)s, %(ts)s, %(vehicle_type)s, %(speed)s, %(confidence)s, %(frame_path)s)
                        """, {
                            'camera_id': 1,
                            'ts': datetime.now(),
                            'vehicle_type': cls_name,
                            'speed': 50.0,
                            'confidence': conf,
                            'frame_path': f"frame_{frame_count}_{track_id}.jpg"
                        })
                    except Exception as e:
                        print(f"Ошибка записи в Bronze: {e}")
                    
                    segment_id = 1
                    minute_key = (segment_id, datetime.now().replace(second=0, microsecond=0))
                    aggregation_cache[minute_key]['speeds'].append(50.0)
                    aggregation_cache[minute_key]['count'] += 1
                    
                    event_data = {
                        'timestamp': datetime.now(),
                        'speed': 50.0,
                        'camera_id': 1,
                        'object_id': track_id if track_id else frame_count,
                        'vehicle_type': cls_name
                    }
                    
                    # ВЫЗОВ check_streaming - передаём ch_client (не cursor)
                    quality_passed = check_streaming(event_data, duplicate_cache, ch_client)
                    if not quality_passed:
                        print(f"Quality check FAILED for {track_id}")
                        aggregation_cache[minute_key]['speeds'].pop()
                        aggregation_cache[minute_key]['count'] -= 1
                        continue
            
            if datetime.now() - last_flush_time >= timedelta(minutes=1):
                flush_aggregates_to_gold()
                last_flush_time = datetime.now()
            
            frame_count += 1
            if frame_count % 30 == 0:
                print(f"Обработано кадров: {frame_count}")
            
            if results[0].boxes is not None:
                annotated_frame = results[0].plot()
                cv2.imshow("YOLO Tracking", annotated_frame)
                if cv2.waitKey(1) & 0xFF == ord("q"):
                    break
                
except Exception as e:
    print(f"Ошибка: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    flush_aggregates_to_gold()
    cv2.destroyAllWindows()
    print(f"\nВсего обработано кадров: {frame_count}")