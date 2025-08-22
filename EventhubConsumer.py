from azure.eventhub import EventHubConsumerClient
from azure.storage.filedatalake import DataLakeServiceClient
import json
from datetime import datetime
from azure.core.exceptions import ResourceNotFoundError



with open("config.json","r") as f:
    config = json.load(f)
EVENT_HUB_NAMESPACE = config["Event_hub_namespace"]
EVENT_HUB_NAME = config["Event_hub_name"]
SharedAccessPolicyName = "RootManageSharedAccessKey"
Shared_access_key = config["Event_hub_shared_access_key"]
EVENT_HUB_CONN = f"Endpoint=sb://{EVENT_HUB_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={SharedAccessPolicyName};SharedAccessKey={Shared_access_key};EntityPath={EVENT_HUB_NAME}"
CONSUMER_GROUP = "$Default"  # default consumer group

#ADLS
storage_account = config["StorageAccount"]
storage_account_key =  config["StorageAccountKey"]
service_client =  DataLakeServiceClient(
    account_url=f"https://{storage_account}.dfs.core.windows.net",
    credential=storage_account_key
)
container = "data"

def save_to_adls(data, file_name):
    file_client = service_client.get_file_client(container, file_name)
    try:    # check if file already exist
        properties = file_client.get_file_properties()     
        current_size = properties.size   
    except ResourceNotFoundError:    # create if file doesn't exist
        file_client.create_file()
        current_size=0

    encoded_data = data.encode('utf-8')
    file_client.append_data(encoded_data,offset=current_size)     # append the current data in the end of file
    file_client.flush_data(current_size+len(encoded_data))       # flush buffered data to the file in ADLS
    

def on_event(partition_context, event):
    message = json.dumps(event.body_as_json())
    #print(message)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    date = datetime.now().strftime("%Y%m%d")
    file_name = f"News_API/Bronze_NewsData/{date}/News_{timestamp}.json"   # Directory+file_name
    save_to_adls(message+"\n", file_name)
    #print(f"{file_name} is created successfully!!")
    partition_context.update_checkpoint(event)  # mark as read

client = EventHubConsumerClient.from_connection_string(
    conn_str=EVENT_HUB_CONN,
    consumer_group=CONSUMER_GROUP,
    eventhub_name=f"{EVENT_HUB_NAME}"
)

with client:
    client.receive(
        on_event=on_event,
       starting_position= "-1"
            )