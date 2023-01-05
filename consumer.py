import json
# import mysql.connector
# mydb = mysql.connector.connect(
#  host="localhost",
#  user="root",
#  password="yash@123",
#  database="kafka"
# )
from kafka import KafkaConsumer
from kafka import KafkaConsumer
print("hi")
if __name__ == '__main__':
    # Kafka Consumer
    consumer = KafkaConsumer(
        'ksqlnew2',
          auto_offset_reset = 'latest', 
    )
    for message in consumer:
        print(json.loads(message.value))
consumer = KafkaConsumer('ksqlnew2')
for message in consumer:
    print(message)
consumer.close()
# cursor = mydb.cursor()
# # cursor.execute("""
# #  INSERT INTO stud_details(sid,name,number,status)
# #  VALUES (%s, %s, %s, %s)
# #  """, (sid,name,number,status)
# cursor.execute("""
#  INSERT INTO stud_details(name,age,weight)
#  VALUES ("yash,12,65")
#  """)
# mydb.commit()
# print("inserted")