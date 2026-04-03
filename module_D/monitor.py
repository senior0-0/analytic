#!/usr/bin/env python3
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import time

class MonitorHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'status': 'ok', 'timestamp': time.time()}
            self.wfile.write(json.dumps(response).encode())
        elif self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'quality_fail_rate': 0, 'total_checks_last_hour': 0}
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        pass  

if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', 8501), MonitorHandler)
    print('Мониторинг запущен на http://0.0.0.0:8501')
    print('Проверьте: curl http://localhost:8501/health')
    server.serve_forever()