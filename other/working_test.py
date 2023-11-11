import asyncio
import websockets
import json
import random
import time

def generate_example_data():
    data = {
            "raceData": {
                "driver": "Max",  
                "speed": random.randint(0, 200),  
                "brake": random.uniform(0, 100) 
            }
        }
    return json.dumps(data)

async def send_data_to_grafana(websocket):
    while True:
        data = generate_example_data()
        print(data)
        await websocket.send(data)
        await asyncio.sleep(0.1) 

if __name__ == "__main__":
    start_server = websockets.serve(send_data_to_grafana, "0.0.0.0", 8777)

    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()

