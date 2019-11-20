# -*- coding: utf-8 -*-
import boto3
import numpy as np
import scipy
import sklearn
import json
import pickle

class Classifier:
    def __init__(self):
        with open("model.pkl", "rb") as f:
            self.model = pickle.load(f)
    def predict(self, data):
        z = self.model.predict([data])
        return z[0]


classifier = Classifier()
client = boto3.client('iot-data', region_name='us-east-2')


def lambda_handler(event, context):
    #TODO1: Get your data

    #eventText = json.loads(event)
    deviceID = event["deviceid"]
    data = np.array(event["data"]).astype(np.float)
    dt = event["datetime"]
    out = classifier.predict(data)

    #TODO2: Send response back to your device
    topic = "hearPred/"+ deviceID
    payload = json.dumps({"prediction": str(out), "datetime": dt})
    response = client.publish(topic=topic, qos =1, payload = payload)

    #TODO3: Send the results to IOT analytics for aggregation
    topic = "hearPred"
    payload = json.dumps({"deviceid":deviceID, "prediction": str(out), "datetime": dt})
    response = client.publish(topic=topic, qos =1, payload = payload)
    
    #TODO4: Send results to a monitor client
