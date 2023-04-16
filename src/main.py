import random
import time
from datetime import datetime
import json
import os

if __name__ == '__main__':

    sensor_path = "../docs/sensors-data/sensor_txt"
    sensor_status_path = "../docs/sensors-data/sensor_status"

    # delete file if exist
    if os.path.exists(sensor_path):
        os.remove(sensor_path)
    if os.path.exists(sensor_status_path):
        os.remove(sensor_status_path)

    sensor_dict = {1: "living_room", 2: "kitchen", 3: "bedroom"}
    sensor_control = {1: "alexa", 2: "alexa", 3: "ok-google"}

    for i in range(1, 10):
        date = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        sensor_data = round(random.random() * 100, 2)
        sensor_id = random.randint(1, 3)
        data_line = json.dumps({"id": sensor_id, "data": sensor_data, "date": date})

        with open(sensor_path, "a") as f:
            f.write(data_line + "\n")
            print(data_line)

        time.sleep(1)

        date = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        sensor_counter = random.randrange(50, 100)
        sensor_cat = {"sensor_id": sensor_id, "sensor_loc": sensor_dict.get(sensor_id),
                      "control": sensor_control.get(sensor_id), "counter": sensor_counter,
                      "date": date
                      }
        sensor_status = json.dumps(sensor_cat)
        with open(sensor_status_path, "a") as f:
            f.write(sensor_status + "\n")
            print(sensor_status)
        time.sleep(1)
