import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

def get_producer():
    while True:
        try:
            print("Connecting to Kafka at localhost:9092...")
            p = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks=1,
                retries=5
            )
            return p
        except Exception as e:
            print(f"Waiting for Kafka to be ready... {e}")
            time.sleep(5)

producer = get_producer()

TOPIC = 'patient-vitals'
PATIENTS = [f"P-{str(i).zfill(3)}" for i in range(1, 21)]

def generate_vitals(patient_id):
    ## Generates a realistic set of 5 diverse metrics.
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    configs = {
        "heart_rate": (60, 100, "bpm"),
        "bp_systolic": (110, 130, "mmHg"),
        "bp_diastolic": (70, 85, "mmHg"),
        "spo2": (95, 100, "%"),
        "temperature": (36.5, 37.5, "C"),
        "respiratory_rate": (12, 18, "bpm")
    }
    
    readings = []
    for metric, (low, high, unit) in configs.items():
        is_abnormal = random.random() > 0.98
        
        if is_abnormal:
            if metric == "spo2": val = random.uniform(85, 92)
            elif metric == "temperature": val = random.uniform(38.5, 40.0)
            else: val = high * random.uniform(1.2, 1.5)
        else:
            val = random.uniform(low, high)
            
        readings.append({
            "patient_id": patient_id,
            "metric_name": metric,
            "value": round(val, 2),
            "unit": unit,
            "timestamp": now
        })
    return readings

def run():
    print(f"Generator started. Sending metrics for {len(PATIENTS)} patients...")
    try:
        while True:
            for p_id in PATIENTS:
                vitals_bundle = generate_vitals(p_id)
                for record in vitals_bundle:
                    producer.send(TOPIC, value=record)
            
            # Ensure data is pushed out of the buffer
            producer.flush()
            print(f"Stream sent at {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(5) # Wait 5 seconds before next stream
    except KeyboardInterrupt:
        print("Generator stopped.")

if __name__ == "__main__":
    run()