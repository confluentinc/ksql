#!/usr/bin/env python3
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Confluent Community License (the "License"); you may not use
# this file except in compliance with the License.  You may obtain a copy of the
# License at
#
# http://www.confluent.io/confluent-community-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OF ANY KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations under the License.
#
#
# Usage: commandTopicConsumer.py [-h] [-f CONFFILE] [-b BROKERS][-k KSQLSERVICEID]
# Command topic consumer that dumps CREATE, DROP and TERMINATE queries to
# stdout. If no arguments are provided, default values are used. Default broker
# is 'localhost:9092'. Default ksqlServiceId is 'default_'. You may optionally
# provide a configuration file with broker specific configuration parameters.
# Every run of this script will consume the topic from the beginning.
# optional arguments:
#   -h, --help        show this help message and exit
#   -b BROKERS        Bootstrap servers
#   -f CONFFILE       Configuration file (configProp=value format)
#   -k KSQLSERVICEID  KsqlDB service ID


from confluent_kafka import KafkaError, KafkaException, version
from confluent_kafka import Producer, Consumer
import json
import logging
import argparse
import uuid
import sys
import re


class CommandRecord (object):
    def __init__(self, stmt):
        self.stmt = stmt

    def __str__(self):
        return "({})".format(self.stmt)

    @classmethod
    def deserialize(cls, binstr):
        d = json.loads(binstr)
        return CommandRecord(d['statement'])

max_offset = -1001
def consumer_latest_offsets(consumer, partitions):
    global max_offset
    for p in partitions:
        high_water = consumer.get_watermark_offsets(p)[1]
        if high_water >= max_offset:
            max_offset = high_water
    print("Max offset in command topic = ", max_offset)

class CommandConsumer(object):
    def __init__(self, ksqlServiceId, conf):
        self.consumer = Consumer(conf)
        self.topic = '_confluent-ksql-{}_command_topic'.format(ksqlServiceId)

    def consumer_run(self):
        global max_offset
        self.consumer.subscribe([self.topic], on_assign=consumer_latest_offsets)
        self.msg_cnt = 0
        self.msg_err_cnt = 0
        stmts = {}
        try:
            while True:
                msg = self.consumer.poll(0.2)
                if msg is None:
                    continue

                if msg.error() is not None:
                    print("consumer: error: {}".format(msg.error()))
                    self.consumer_err_cnt += 1
                    continue

                try:
                    #print("Read msg with offset ", msg.offset())
                    self.msg_cnt += 1
                    record = CommandRecord.deserialize(msg.value())
                    #print(record)

                    # match statements CREATE/DROP STREAM, CREATE/DROP TABLE
                    match = re.search(r'(?:create|drop) (?:stream|table) ([a-zA-z0-9-]+?)(:?\(|AS|\s|;)', record.stmt, re.I)
                    if match:
                        name = match.group(1).upper()
                        if name == "KSQL_PROCESSING_LOG":
                            continue
                        if name not in stmts:
                            stmts[name] = []
                        stmts[name].append(record.stmt)

                    # match statements TERMINATE query
                    match2 = re.search(r'(?:terminate) (?:ctas|csas)_(.+?)_', record.stmt, re.I)
                    if match2:
                        name = match2.group(1).upper()
                        stmts[name].append(record.stmt)

                    # match statements INSERT INTO stream or table
                    match3 = re.search(r'(?:insert into) ([a-zA-z0-9-]+?)(:?\(|\s|\()', record.stmt, re.I)
                    if match3:
                        name = match3.group(1).upper()
                        stmts[name].append(record.stmt)

                    #match statements CREATE TYPE
                    match4 = re.search(r'(?:create|drop) type ([a-zA-z0-9-]+?)(:?AS|\s|;)', record.stmt, re.I)
                    if match4:
                        name = match4.group(1).upper()
                        if name not in stmts:
                            stmts[name] = []
                        stmts[name].append(record.stmt)

                    if match is None and match2 is None and match3 is None and match4 is None:
                        print("No match for record %s",record)

                    # High watermark is +1 from last offset
                    if msg.offset() >= max_offset-1:
                        break

                except ValueError as ex:
                    print("consumer: Failed to deserialize message in "
                                     "{} [{}] at offset {} (headers {}): {}".format(
                                         msg.topic(), msg.partition(), msg.offset(), msg.headers(), ex))
                    self.msg_err_cnt += 1

        except KeyboardInterrupt:
            pass

        finally:
            self.consumer.close()
            print("Consumed {} messages, erroneous message = {}.".format(self.msg_cnt, self.msg_err_cnt))
            for key, value in stmts.items():
                print("[{}]".format(key))
                for cmd in value:
                    print("> {}".format(cmd))
                print("-------------------------------------------------\n")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Command topic consumer that dumps CREATE, DROP and TERMINATE queries to '+
        'stdout. If no arguments are provided, default values are used. Default broker is '
        '\'localhost:9092\'. Default ksqlServiceId is \'default_\'. You may optionally provide a configuration file with '+
        'broker specific configuration parameters. Every run of this script will consume the topic from the beginning. ')

    parser.add_argument('-f', dest='confFile', type=argparse.FileType('r'),
                        help='Configuration file (configProp=value format)')
    parser.add_argument('-b', dest='brokers', type=str, default=None, help='Bootstrap servers')
    parser.add_argument('-k', dest='ksqlServiceId', type=str, default=None, help='KsqlDB service ID')

    args = parser.parse_args()

    conf = dict()
    if args.confFile is not None:
        # Parse client configuration file
        for line in args.confFile:
            line = line.strip()
            if len(line) == 0 or line[0] == '#':
                continue

            i = line.find('=')
            if i <= 0:
                raise ValueError("Configuration lines must be `name=value..`, not {}".format(line))

            name = line[:i]
            value = line[i+1:]

            conf[name] = value

    if args.brokers is not None:
        # Overwrite any brokers specified in configuration file with
        # brokers from -b command line argument
        conf['bootstrap.servers'] = args.brokers
    elif 'bootstrap.servers' not in conf:
        conf['bootstrap.servers'] = 'localhost:9092'


    if 'group.id' not in conf:
        # Generate a unique group.id 
        conf['group.id'] = 'commandTopicConsumer.py-{}'.format(uuid.uuid4())

    if args.ksqlServiceId is None:
        args.ksqlServiceId = 'default_'   

    conf['auto.offset.reset'] = 'earliest'    
    conf['enable.auto.commit']= 'False'
    conf['client.id'] = 'commandClient'

    c = CommandConsumer(args.ksqlServiceId, conf)
    c.consumer_run()
