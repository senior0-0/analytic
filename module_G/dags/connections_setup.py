from airflow.models import Connection
from airflow import settings

def create_connection():
    conn = Connection(
        conn_id='transport_dwh',
        conn_type='postgres',
        host='localhost',  
        schema='db',
        login='user',
        password='0000',
        port=5432
    )
    session = settings.Session()
    if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()
        print(f"Connection '{conn.conn_id}' created")
    else:
        print(f"Connection '{conn.conn_id}' already exists")
    session.close()

if __name__ == "__main__":
    create_connection()