from flask import Flask, jsonify
import psycopg2
import time

app = Flask(__name__)
DB = "dbname=transport user=admin password=pass host=localhost"

@app.route('/health')
def health():
    return jsonify({"status": "ok", "timestamp": time.time()})

@app.route('/metrics')
def metrics():
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN passed THEN 0 ELSE 1 END) as failed
        FROM data_quality_log 
        WHERE timestamp > NOW() - INTERVAL '1 hour'
    """)
    total, failed = cur.fetchone()
    
    cur.close()
    conn.close()
    
    return jsonify({
        "quality_fail_rate": failed / total if total > 0 else 0,
        "total_checks_last_hour": total
    })

if __name__ == '__main__':
    app.run(port=8501)