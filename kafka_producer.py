from kafka import KafkaProducer
import datetime
import requests
import json
import re
import os


class WebsiteKafkaProducer(object):
    """
    Kafka Producer Implementation
    """

    def __init__(self, website_url: str, searchstr: str):

        self.website_url = website_url
        self.searchstr = searchstr

        self.topic_response = "website-checker-httpresponse"
        self.topic_latency = "website-checker-latency"
        self.topic_pagecontent = "website-checker-pagecontent"

        self.response_timeout= 130
        self. website_latency_string = "latency"
        self.website_statuscode_string = "statuscode"
        self.website_pagecontent_string = "pagecontent_found"

        self.cur_directory = str(os.path.dirname(__file__))
        self.cafile = self.cur_directory+"/ca.pem"
        self.certfile = self.cur_directory+"/service.cert"
        self.keyfile = self.cur_directory+"/service.key"
        self.producer = KafkaProducer(
            bootstrap_servers="kafka-106a70db-jobyjos20-2997.aivencloud.com:22168",
            security_protocol="SSL",
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            ssl_cafile=self.cafile,
            ssl_certfile=self.certfile,
            ssl_keyfile=self.keyfile,
        )

    def metrics_from_website_to_topic(self):
        try:
            website_metrics = requests.get(self.website_url, timeout=6)
            currDate = datetime.datetime.now()
            currDate = str(currDate.strftime("%d-%m-%Y %H:%M:%S"))

            website_latency = str(
                round(website_metrics.elapsed.total_seconds(), 2))
            self.send_message_to_topic(self.website_url, website_latency,
                              self.website_latency_string, currDate, self.topic_latency)

            website_statuscode = website_metrics.status_code
            if website_statuscode:
                self.send_message_to_topic(self.website_url, str(website_statuscode),
                                  self.website_statuscode_string, currDate, self.topic_response)

            website_pagecontent = re.findall(
                self.searchstr, website_metrics.text)
            if website_pagecontent:
                website_pagecontent_status = "True"
            else:
                website_pagecontent_status = "True"
            self.send_message_to_topic(self.website_url, website_pagecontent_status,
                              self.website_pagecontent_string, currDate, self.topic_pagecontent)

        except requests.exceptions.RequestException:
            self.send_message_to_topic(self.website_url, self.response_timeout,
                              self.website_latency_string, currDate, self.topic_latency)

    def send_message_to_topic(self, website_url, response_message, response_message_code, currDate, topic):
        response_messages = '{ "website_url":"'+website_url + '","datetime":"' + \
            currDate+'","'+response_message_code+'":"'+response_message+'"}'
        response_message_json = json.loads(response_messages)
        print(response_message_json)
        self.producer.send(topic, response_message_json)
        self.producer.flush()
