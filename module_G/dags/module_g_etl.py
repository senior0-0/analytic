from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import random
from sqlalchemy import create_engine
import json

def get_engine():
    hook = PostgresHook(postgres_conn_id='transport_dwh')
    return hook.get_sqlalchemy_engine()

default_args = {
    'owner': 'analyst',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'module_g_transport_etl',
    default_args=default_args,
    description='ETL пайплайн для транспортных данных (очистка, обогащение, витрины)',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
)

def extract_raw_data(**context):
    from datetime import datetime  
    
    engine = get_engine()  
    
    data = []
    cameras = ['CAM_01', 'CAM_02', 'CAM_03', 'CAM_04', 'CAM_05']
    vehicle_types = ['car', 'truck', 'bus', 'motorcycle', 'bicycle']
    
    for _ in range(100):
        detected_at = datetime.now() - timedelta(minutes=random.randint(0, 60))
        data.append({
            'camera_id': random.choice(cameras),
            'vehicle_type': random.choice(vehicle_types),
            'speed_kmh': round(random.uniform(0, 140), 1),
            'detected_at': detected_at,
            'raw_payload': json.dumps({
                'confidence': round(random.uniform(0.7, 0.99), 2),
                'bbox': [random.randint(0, 1920), random.randint(0, 1080)] * 2
            })
        })
    
    df = pd.DataFrame(data)
    df.to_sql('raw_detections', engine, schema='staging', if_exists='append', index=False)
    
    context['task_instance'].xcom_push(key='records_extracted', value=len(data))
    print(f"Загружено {len(data)} сырых записей в staging.raw_detections")

def clean_and_validate_data(**context):
    engine = get_engine()
    
    with engine.connect() as conn:
        conn.execute("""
            DELETE FROM staging.raw_detections a
            USING staging.raw_detections b
            WHERE a.detection_id < b.detection_id
              AND a.camera_id = b.camera_id
              AND a.detected_at = b.detected_at
              AND a.vehicle_type = b.vehicle_type;
        """)
        
        conn.execute("""
            INSERT INTO core.cleaned_detections (camera_id, vehicle_type, speed_kmh, detected_at, is_valid, quality_issues)
            SELECT 
                camera_id,
                vehicle_type,
                speed_kmh,
                detected_at,
                CASE 
                    WHEN speed_kmh BETWEEN 0 AND 180 THEN TRUE 
                    ELSE FALSE 
                END AS is_valid,
                CASE 
                    WHEN speed_kmh < 0 THEN ARRAY['negative_speed']
                    WHEN speed_kmh > 180 THEN ARRAY['excessive_speed']
                    ELSE ARRAY[]::TEXT[]
                END AS quality_issues
            FROM staging.raw_detections;
        """)
        
        conn.execute("TRUNCATE staging.raw_detections;")
        
        conn.commit()
    
    print("Данные очищены и перенесены в core.cleaned_detections")

def quality_audit(**context):
    engine = get_engine()
    
    with engine.connect() as conn:
        dup_result = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT camera_id, detected_at, vehicle_type, COUNT(*)
                FROM core.cleaned_detections
                GROUP BY 1, 2, 3
                HAVING COUNT(*) > 1
            ) duplicates;
        """).scalar()
        
        invalid_speed = conn.execute("""
            SELECT COUNT(*) FROM core.cleaned_detections 
            WHERE is_valid = FALSE;
        """).scalar()
        

        conn.execute("""
            INSERT INTO core.data_quality_audit (check_name, table_name, detected_at, issue_count, details)
            VALUES 
                ('duplicate_check', 'core.cleaned_detections', NOW(), %s, %s),
                ('speed_validation', 'core.cleaned_detections', NOW(), %s, %s);
        """, (
            dup_result, json.dumps({'duplicate_groups': dup_result}),
            invalid_speed, json.dumps({'invalid_speed_count': invalid_speed})
        ))
        conn.commit()
    
    print(f"Аудит завершён: дубликатов={dup_result}, невалидных скоростей={invalid_speed}")

def enrich_with_external_factors(**context):
    engine = get_engine()
    
    with engine.connect() as conn:
        conn.execute("""
            INSERT INTO core.enriched_detections (camera_id, vehicle_type, speed_kmh, detected_at, temperature_c, weather_condition, is_weekend, hour_of_day)
            SELECT 
                camera_id,
                vehicle_type,
                speed_kmh,
                detected_at,
                -- Имитация температуры на основе часа
                CASE 
                    WHEN EXTRACT(HOUR FROM detected_at) BETWEEN 6 AND 18 THEN 15 + random()*10
                    ELSE 5 + random()*5
                END AS temperature_c,
                -- Имитация погоды
                CASE 
                    WHEN EXTRACT(HOUR FROM detected_at) BETWEEN 6 AND 18 THEN 'clear'
                    ELSE 'cloudy'
                END AS weather_condition,
                EXTRACT(DOW FROM detected_at) IN (0, 6) AS is_weekend,
                EXTRACT(HOUR FROM detected_at) AS hour_of_day
            FROM core.cleaned_detections
            WHERE is_valid = TRUE;
        """)
        conn.commit()
    
    print("Данные обогащены внешними факторами")

def update_aggregated_marts(**context):
    engine = get_engine()
    
    with engine.connect() as conn:
        conn.execute("""
            INSERT INTO mart.hourly_traffic (hour, camera_id, total_vehicles, avg_speed_kmh, primary_vehicle_type, updated_at)
            SELECT 
                DATE_TRUNC('hour', detected_at) AS hour,
                camera_id,
                COUNT(*) AS total_vehicles,
                ROUND(AVG(speed_kmh), 1) AS avg_speed_kmh,
                MODE() WITHIN GROUP (ORDER BY vehicle_type) AS primary_vehicle_type,
                NOW()
            FROM core.enriched_detections
            GROUP BY 1, 2
            ON CONFLICT (hour, camera_id) DO UPDATE SET
                total_vehicles = EXCLUDED.total_vehicles,
                avg_speed_kmh = EXCLUDED.avg_speed_kmh,
                primary_vehicle_type = EXCLUDED.primary_vehicle_type,
                updated_at = EXCLUDED.updated_at;
        """)
        conn.commit()
    
    print("Витрины обновлены")

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

extract = PythonOperator(
    task_id='extract_raw_data',
    python_callable=extract_raw_data,
    dag=dag
)

clean = PythonOperator(
    task_id='clean_and_validate',
    python_callable=clean_and_validate_data,
    dag=dag
)

audit = PythonOperator(
    task_id='quality_audit',
    python_callable=quality_audit,
    dag=dag
)

enrich = PythonOperator(
    task_id='enrich_with_factors',
    python_callable=enrich_with_external_factors,
    dag=dag
)

update_marts = PythonOperator(
    task_id='update_aggregated_marts',
    python_callable=update_aggregated_marts,
    dag=dag
)

start >> extract >> clean >> audit >> enrich >> update_marts >> end