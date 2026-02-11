#### Run Primary Node (Run in new terminal):

```bash
uv run primary_node.py --host 127.0.0.1 --port 9200 --grpc-port 50050
```

### Run Seconday Node 1 (Run in new terminal):

```bash
uv run secondary_node.py --host 127.0.0.1 --port 9100 --grpc-port 50051 --coordinator-grpc 127.0.0.1:50050 --node-id worker1
```

### Run Secondary Node 2 (Run in new terminal):
```bash
uv run secondary_node.py --host 127.0.0.1 --port 9101 --grpc-port 50052 --coordinator-grpc 127.0.0.1:50050 --node-id worker2
```


### Run With GRPC (Run in new terminal):

```bash
# change protocol flag to switch between http and grpc
uv run  primes_cli.py --low 0 --high 1000000 --mode count --exec distributed \ 
  --primary http://127.0.0.1:9200 --protocol http --time

uv run  primes_cli.py --low 0 --high 1000000 --mode count --exec distributed \ 
  --primary http://127.0.0.1:9200 --protocol grpc --time

```