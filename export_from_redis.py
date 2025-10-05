import redis
import json
import sys

# Connection to Redis in Docker (localhost from host)
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def serialize_redis_value(key, key_type):
    """Serializes Redis value to Python object for JSON (direct format)."""
    if key_type == 'string':
        return r.get(key) or ''
    elif key_type == 'hash':
        return dict(r.hgetall(key))
    elif key_type == 'list':
        return r.lrange(key, 0, -1)
    elif key_type == 'set':
        return sorted(list(r.smembers(key)))  # Sorted for stability
    elif key_type == 'zset':
        return dict(r.zrange(key, 0, -1, withscores=True))  # {member: score}
    elif key_type == 'stream':
        entries = r.xread({key: '0'}, block=0, count=10000)
        if entries:
            return [[entry_id, dict(fields)] for _, entries_list in entries for entry_id, fields in entries_list]
        return []
    else:
        # Fallback for other types (module, bitmap, etc.) — as string with type
        raw_value = r.dump(key)
        return f"{key_type}: (binary data, size {len(raw_value) if raw_value else 0} bytes)"

    return None  # Should not reach

def export_to_json(output_file):
    """Exports all keys to JSON file (direct format)."""
    data = {}
    cursor = 0
    processed = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, count=100)
        for key in keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            key_type_obj = r.type(key)
            key_type = key_type_obj.decode('utf-8') if isinstance(key_type_obj, bytes) else key_type_obj
            value = serialize_redis_value(key_str, key_type)
            data[key_str] = value  # Direct value — no wrapper!
            processed += 1
            if processed % 1000 == 0:
                print(f"Processed keys: {processed}")

        if cursor == 0:
            break

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Export completed. Processed {len(data)} keys. Data saved to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 export_from_redis.py <output_file.json>")
        sys.exit(1)

    try:
        export_to_json(sys.argv[1])
    except redis.exceptions.ConnectionError as e:
        print(f"Redis connection error: {e}. Check if the Docker container is running.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
