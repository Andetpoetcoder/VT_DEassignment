from kafka import KafkaProducer
import csv,json,time

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
with open('Data/log_action.csv', 'r') as file:
    fieldnames = ['student_code', 'activity', 'num_file', 'time']
    reader = csv.DictReader(file, fieldnames = fieldnames)
    for row in reader:
        row['num_file'] = int(row['num_file'])
        row['student_code'] = int(row['student_code'])
        producer.send('VDT2024', row)
        time.sleep(1)
producer.flush()