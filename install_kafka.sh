#!/bin/bash

# Atualizar os pacotes
sudo apt update

# Instalar o Java (Kafka requer Java)
sudo apt install default-jre -y

# Baixar e extrair o Apache Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
mv kafka_2.13-3.6.0/* /opt/kafka
rm kafka_2.13-3.6.0.tgz
rmdir kafka_2.13-3.6.0

# Configurar variáveis de ambiente
echo "export KAFKA_HOME=/opt/kafka" >> ~/.bashrc
echo "export PATH=\$PATH:\$KAFKA_HOME/bin" >> ~/.bashrc
source ~/.bashrc

# Iniciar o servidor ZooKeeper (necessário para o Kafka)
nohup zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties > /dev/null 2>&1 &

# Esperar alguns segundos para o ZooKeeper iniciar completamente
sleep 5

# Iniciar o servidor Kafka
nohup kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties > /dev/null 2>&1 &

# Esperar alguns segundos para o Kafka iniciar completamente
sleep 5

# Criar um tópico de exemplo
kafka-topics.sh --create --topic sensor_readings --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

echo "Instalação concluída. Kafka está em execução e pronto para uso."
