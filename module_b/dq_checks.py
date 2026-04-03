from datetime import datetime

def check_streaming(event_data, duplicate_cache, ch_client, write_quality_log_func=None):
    details_list = []
    passed = True
    
    timestamp = event_data.get('timestamp')
    if timestamp:
        now = datetime.now()
        time_diff = abs((now - timestamp).total_seconds())
        if time_diff > 60:  
            passed = False
            details_list.append(f"timestamp_lag: {time_diff:.1f}s")
        elif time_diff < -5:  
            passed = False
            details_list.append(f"timestamp_future: {time_diff:.1f}s")
    
    speed = event_data.get('speed', 0)
    if speed < 0:
        passed = False
        details_list.append(f"negative_speed: {speed}")
    elif speed > 180:
        passed = False
        details_list.append(f"excessive_speed: {speed}")
    
    object_id = event_data.get('object_id')
    if object_id:
        duplicate_key = f"{event_data.get('camera_id')}_{object_id}_{timestamp}"
        if duplicate_key in duplicate_cache:
            passed = False
            details_list.append(f"duplicate_detection: {object_id}")
        else:
            duplicate_cache.add(duplicate_key)
            if len(duplicate_cache) > 10000:
                duplicate_cache.clear()

    vehicle_type = event_data.get('vehicle_type', '')
    valid_types = ['car', 'truck', 'bus', 'motorcycle', 'bicycle', 'person']
    if vehicle_type not in valid_types:
        passed = False
        details_list.append(f"invalid_vehicle_type: {vehicle_type}")
    
    confidence = event_data.get('confidence', 1.0)
    if confidence < 0.3:
        passed = False
        details_list.append(f"low_confidence: {confidence:.2f}")
    
    details = "; ".join(details_list) if details_list else "all_checks_passed"
    
    if write_quality_log_func and not passed:
        write_quality_log_func(
            pipeline_name="streaming_processor",
            check_name="comprehensive_quality",
            passed=passed,
            details=details,
            camera_id=event_data.get('camera_id'),
            object_id=str(event_data.get('object_id')),
            severity=2 if not passed else 0
        )
    
    return passed, details