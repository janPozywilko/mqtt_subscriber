import json
import base64

import paho.mqtt.client as mqtt

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# This is data used for initializing MQTT Client
hostname = "eu1.cloud.thethings.network"
topic = "#"
port = 1883
auth={'username':"demo-v3-application",'password':"NNSXS.MUC4M7BKHJES6KLD3AMW4CNWNVP6FYT6Z2VCVRQ.LPCFTOCP7NEFLTDI42JN5PV2IVUZ5LNCHLIPFU3ZO4UFKELERJZA"}

# This is data for initializing InfluxDB2.0 Client
bucket = "testing"
org = "8e6aa7bc7f6bec91"
token = "5Pb0iOzMedOHRzomYYrLWi7yuvdBM2Rkm7CwGmMSIYjs6gVliOYgUqmDXR6H9e5ZEPq1SypgY-peeVr4u-RyTA=="
url="http://62.3.170.222:8086"



def connect_influx() -> influxdb_client:

    influx_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
    
    return influx_client

def connect_mqtt() -> mqtt:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
                print("Connected to MQTT Broker!!!")
                client.subscribe("#")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt.Client()
    client.on_connect = on_connect

    client.username_pw_set(auth['username'], password=auth['password'])
    client.connect(hostname, port=1883)
    
    return client

gcs_keys = {"L":"200","H":"500","P":"10000","C":"2","F":"1","R":"5000"}.keys()
gic_keys = {"irrigationCounter":"327"}.keys()
status_keys = {"CV":"4735","CS":"0","PV":"13236","PS":"6","WL":"425"}.keys()
measurement_keys = {"t":"24.47","h":"27.37","l":"4.75","g":"22.7","m":"16.6"}.keys()

def subscribe(client: mqtt, influx_client: influxdb_client):

    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    def on_message(client, userdata, msg):
        try:
            payload_decoded = json.loads(msg.payload.decode('utf-8'))

            dev_eui = payload_decoded['end_device_ids']['dev_eui']

            frm_payload_encrypted = payload_decoded['uplink_message']['frm_payload']
            frm_payload_decoded = base64.b64decode(frm_payload_encrypted)

            frm_string = str(frm_payload_decoded, "utf-8")

            frm_as_dict = json.loads(frm_string)

            keys = frm_as_dict.keys()

            if keys == measurement_keys:
                try:
                    t = float(frm_as_dict.get('t'))
                    h = float(frm_as_dict.get('h'))
                    l = float(frm_as_dict.get('l'))
                    g = float(frm_as_dict.get('g'))
                    m = float(frm_as_dict.get('m'))
                    p = influxdb_client.Point("plantbox").tag("dev-eui", dev_eui).field("temperature", t).field("humidity", h).field("light", l).field("ground_temperature", g).field("moisture", m)
                    write_api.write(bucket=bucket, org=org, record=p)
                    print(f"send measurement data to influxdb {dev_eui}")
                except:
                   print("fail to send measurement data to influx db")
            elif keys == gcs_keys:
                try:
                    L = float(frm_as_dict.get('L'))
                    H = float(frm_as_dict.get('H'))
                    P = float(frm_as_dict.get('P'))
                    C = float(frm_as_dict.get('C'))
                    F = float(frm_as_dict.get('F'))
                    R = float(frm_as_dict.get('R'))
                    p = influxdb_client.Point("plantbox").tag("dev-eui", dev_eui).field("L", L).field("H", H).field("P", P).field("C", C).field("F", F).field("R", R)
                    write_api.write(bucket=bucket, org=org, record=p)
                    print(f"send GCS data to influxdb {dev_eui}")
                except:
                    print("fail to send GCS data to influx db")
            elif keys == gic_keys:
                try:
                    irrigation_counter = float(frm_as_dict.get('irrigationCounter'))
                    p = influxdb_client.Point("plantbox").tag("dev-eui", dev_eui).field("irrigationCounter", irrigation_counter)
                    write_api.write(bucket=bucket, org=org, record=p)
                    print(f"send GIC data to influxdb {dev_eui}")
                except:
                    print("fail to send GIC data to influx db")
            elif keys == status_keys:
                try:
                    CV = float(frm_as_dict.get('CV'))
                    CS = float(frm_as_dict.get('CS'))
                    PV = float(frm_as_dict.get('PV'))
                    PS = float(frm_as_dict.get('PS'))
                    WL = float(frm_as_dict.get('WL'))
                    p = influxdb_client.Point("plantbox").tag("dev-eui", dev_eui).field("CV", CV).field("CS", CS).field("PV", PV).field("PS", PS).field("WL", WL)
                    write_api.write(bucket=bucket, org=org, record=p)
                    print(f"send status data to influxdb {dev_eui}")
                except:
                    print("fail to send status data to influx db")
        except:
            print(msg.payload.decode('utf-8'))

    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    influx_client = connect_influx()
    subscribe(client, influx_client)
    client.loop_forever()


if __name__ == '__main__':
    run()

