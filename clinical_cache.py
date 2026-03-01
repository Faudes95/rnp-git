import json
import redis

r = redis.Redis(host="localhost", port=6379)


def cache_patient(patient_id, data, ttl=300):
    r.setex(f"patient:{patient_id}", ttl, json.dumps(data, ensure_ascii=False))


def get_cached_patient(patient_id):
    d = r.get(f"patient:{patient_id}")
    return json.loads(d) if d else None
