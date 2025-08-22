import requests
import json
from azure.eventhub import EventHubProducerClient, EventData

with open("config.json","r") as f:
    config = json.load(f)
News_API_key = config["NewsApiKey"]
News_API_URL = f"https://newsapi.org/v2/top-headlines?country=us&apiKey={News_API_key}"

EVENT_HUB_NAMESPACE = config["Event_hub_namespace"]
EVENT_HUB_NAME = config["Event_hub_name"]
SharedAccessPolicyName = "RootManageSharedAccessKey"
Shared_access_key = config["Event_hub_shared_access_key"]
EVENT_HUB_CONN = f"Endpoint=sb://{EVENT_HUB_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={SharedAccessPolicyName};SharedAccessKey={Shared_access_key};EntityPath={EVENT_HUB_NAME}"

def fetch_news():
    response = requests.get(News_API_URL)
    if response.status_code==200:
        return response.json().get("articles",[])
    else:
        return response.json("message",[])
def send_to_eventhub(news_articles):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONN,
        eventhub_name=EVENT_HUB_NAME
    )

    #create batch
    event_data_batch = producer.create_batch()
    for article in news_articles:
        try:
            event = json.dumps(article)    # convert each article to json string
            event_data_batch.add(EventData(event))
        except ValueError:
            producer.send_batch(event_data_batch)    
            # as batch has limit of 1MB, 
            # if add() gets error, we send current data and create another batch
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(event))

    if len(event_data_batch)>0:
        producer.send_batch(event_data_batch)

    producer.close()
    print(f"Sent {len(news_articles)} articles to Event Hub")

if __name__ == "__main__":
    articles = fetch_news()
    send_to_eventhub(articles)