#!/bin/bash

# Start Zookeeper
echo "Starting Zookeeper..."
gnome-terminal -- bash -c 'cd ~/kafka && bin/zookeeper-server-start.sh config/zookeeper.properties; sleep 3; read -p "Press Enter to close this terminal"'

# Start Kafka server
echo "Starting Kafka server..."
gnome-terminal -- bash -c 'cd ~/kafka && bin/kafka-server-start.sh config/server.properties; sleep 3; read -p "Press Enter to close this terminal"'

# Start producer
echo "Starting producer..."
gnome-terminal -- bash -c 'cd ~/Documents && python3 producer.py; sleep 3; read -p "Press Enter to close this terminal"'

# Start consumers
echo "Starting consumer 1..."
gnome-terminal -- bash -c 'cd ~/Documents && python3 apriori.py; sleep 3; read -p "Press Enter to close this terminal"'
echo "Starting consumer 2..."
gnome-terminal -- bash -c 'cd ~/Documents && python3 pcy.py; sleep 3; read -p "Press Enter to close this terminal"'
echo "Starting consumer 3..."
gnome-terminal -- bash -c 'cd ~/Documents && python3 anomoly.py; sleep 3; read -p "Press Enter to close this terminal"'

echo "All components started successfully."

