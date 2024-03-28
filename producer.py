import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
# mydb = mysql.connector.connect(
#  host="localhost",
#  user="root",
#  password="yash@123",
#  database="prac11"
# )
data = """{'name':'yash','age' : 10},
          {'name':'soham','age' : 7},
          {'name':'mahesh','age' : 8},       
          {'name':'talati','age' : 17}, 
          {'name':'dhrit','age' : 21},                    
          {'name' :'vikas','age' : 15}"""
result = '{"name":"admin"},{"age","21"}'
result = json.loads(result)
# y = json.loads(data)
# id= list(map(itemgetter('id'), data))
# age=list(map(itemgetter('age'), data))
# weight=list(map(itemgetter('weight'), data))
time_to_sleep = 2
def serializer(message):
    return json.dumps(message).encode('utf-8')
    # return message.encode('utf-8')


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)
# for i, id in enumerate(json1["members"]):
#         if i%2==0:
#             # print(f"{i}:{id}")
#             u1=[]
#             # new2=[*id.values()]
#             u1.append(', '.join(str(x) for x in id.values()))
#             # print(type(id))
#             print(u1)
if __name__ == '__main__':
    # Infinite loop - runs until you kill the program
    while True:
        # for i, id in enumerate(json1["members"]):
        #     if i%2==0:
        #         # print(f"{i}:{id}")
        #         u1=[]
        #         # new2=[*id.values()]
        #         u1.append(', '.join(str(x) for x in id.values()))
        #         # print(type(id))
        #         print(u1)
        #         producer.send('ksql', u1)
        # for (a, b, c) in zip(id, age, weight):
        #     list1=[]
        #     list1.extend([a,b,c])
        #     producer.send('ksql', a)
        #     time.sleep(time_to_sleep)
        #     producer.send('ksql', b)
        #     producer.send('ksql', c)
        #     producer.flush(30)\
        producer.send('f',data)
        time.sleep(time_to_sleep)
        # Sleep for a random number of seconds
