# Step 1. Download Docker Image

 [official docker image of cassandra](https://hub.docker.com/_/cassandra)
```
docker pull cassandra
```

# Step 2. Start a cassandra sever instance
```
docker run -d -p 9042:9042 --name test-cassandra-v2 cassandra
```
    If you have already created the container, but it is stopped, use `docker start <image name> OR <container id>`.

# Step 2. Install package
```
pip3 install cassandra-driver
```

# Step 3. Run python file
```
python .py
```