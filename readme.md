## Kafka Challenge

The file unique_users.py reads data from kafka topic and writes unique users per minute to another kafka topic. It is a protoype of the [Data Engineering Challenge](https://github.com/tamediadigital/hiring-challenges/tree/master/data-engineer-challenge). \
The file requirements.txt contains the required packages and the steps described below are necessary to run the code on OS X.

#### Create virtual environment (with python version 3.8) and install requirements
pip install -r requirements.txt

#### Start Kafka locally
export CONFLUENT_HOME=/usr/local/Cellar/confluent-7.2.2/  \
confluent local services kafka start

#### Create topics 
kafka-topics --create --topic website_visits --bootstrap-server localhost:9092  \
kafka-topics --create --topic unique_website_visits_per_minute --bootstrap-server localhost:9092


#### Input data to kafka topic (Data is available [here](https://tda-public.s3.eu-central-1.amazonaws.com/hire-challenge/stream.jsonl.gz))
gzcat stream.jsonl.gz | kafka-console-producer --broker-list localhost:9092 --topic website_visits


#### Run python file (in venv): 
python unique_user.py 


#### Check messages in website_visits_per_minute
kafka-console-consumer --bootstrap-server localhost:9092 --topic unique_website_visits_per_minute --from-beginning
