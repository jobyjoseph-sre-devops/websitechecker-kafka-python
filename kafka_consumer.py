from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
from datetime import datetime
import psycopg2
import sys
import json
import os


class WebsiteKafkaConsumer(object):
    """
    Kafka consumer implementation

    """

    def __init__(self):
        self.cur_directory = str(os.path.dirname(__file__))
        self.cafile = self.cur_directory+"/ca.pem"
        self.certfile = self.cur_directory+"/service.cert"
        self.keyfile = self.cur_directory+"/service.key"
        self.bootstrap_servers = "kafka-106a70db-jobyjos20-2997.aivencloud.com:22168"
        self.topic_latency = "website-checker-latency"
        self.topic_httpresponse = "website-checker-httpresponse"
        self.topic_pagecontent = "website-checker-pagecontent"
        self.auto_offset_reset = "earliest"
        self.client_id = "demo-client-1"
        self.group_id = "demo-group"
        self.security_protocol = "SSL"

        self.uri = "postgres://avnadmin:s2cpa84kngkt86ct@pg-website-checker-jobyjos20-2997.aivencloud.com:22166/defaultdb?sslmode=require"
        self.db_conn = psycopg2.connect(self.uri)
        self.connection = self.db_conn.cursor(cursor_factory=RealDictCursor)

        self.create_latency_table = """ 
        CREATE TABLE IF NOT EXISTS website_checker_latency (
        id SERIAL PRIMARY KEY,
        check_time TIMESTAMP NOT NULL,
        data jsonb
        ); 
        """
        self.create_httpresponse_table = """ 
        CREATE TABLE IF NOT EXISTS website_checker_httpresponse (
        id SERIAL PRIMARY KEY,
        check_time TIMESTAMP NOT NULL,
        data jsonb
        ); 
        """
        self.create_pagecontent_table = """ 
        CREATE TABLE IF NOT EXISTS website_checker_pagecontent (
        id SERIAL PRIMARY KEY,
        check_time TIMESTAMP NOT NULL,
        data jsonb
        ); 
        """

    def messages_from_topic_to_database(self):
        try:
            latency_consumer = self.create_consumer(self.topic_latency)
            httpresponse_consumer = self.create_consumer(
                self.topic_httpresponse)
            pagecontent_consumer = self.create_consumer(self.topic_pagecontent)

            
            self.create_postgress_table(self.connection, self.create_latency_table)
            self.create_postgress_table(
                self.connection, self.create_httpresponse_table)
            self.create_postgress_table(
                self.connection, self.create_pagecontent_table)

            insert_latency_query = """ INSERT INTO website_checker_latency (check_time,data) VALUES (%s,%s)"""
            self.insert_messages_into_postgress_table(
                self.connection, latency_consumer, insert_latency_query)

            insert_httpresponse_query = """ INSERT INTO website_checker_httpresponse (check_time,data) VALUES (%s,%s)"""
            self.insert_messages_into_postgress_table(
                self.connection, httpresponse_consumer, insert_httpresponse_query)

            insert_pagecontent_query = """ INSERT INTO website_checker_pagecontent (check_time,data) VALUES (%s,%s)"""
            self.insert_messages_into_postgress_table(
                self.connection, pagecontent_consumer, insert_pagecontent_query)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def create_consumer(self, topic):
        consumer = KafkaConsumer(
            topic,
            auto_offset_reset=self.auto_offset_reset,
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            group_id=self.group_id,
            security_protocol=self.security_protocol,
            ssl_cafile=self.cafile,
            ssl_certfile=self.certfile,
            ssl_keyfile=self.keyfile,
        )
        return consumer

    def create_postgress_table(self, connection, create_table_query):
        connection.execute(create_table_query)

    def insert_messages_into_postgress_table(self, connection, consumer, insert_query):
        for _ in range(2):
            raw_msgs = consumer.poll(timeout_ms=1000)
            for tp, raw_msg in raw_msgs.items():
                for msg in raw_msg:
                    json_msg = json.loads(msg.value)
                    date_time_obj = datetime.strptime(
                        json_msg["datetime"], "%d-%m-%Y %H:%M:%S")
                    record_to_insert = (date_time_obj,
                                      json.dumps(json_msg))
                    connection.execute(insert_query, record_to_insert)
                    consumer.commit()
