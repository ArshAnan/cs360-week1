import requests as r
from pprint import pprint

primary_node_url = "http://127.0.0.1:9200"
print("Terminal to Interact with Primary Node")
print("Supported Commands: h - health, n - nodes, r - register, c - compute, q - quit")
print()
command = input("Type in a Command: ")
while command.lower() != "q":
    response = {}
    match command.lower():
        case "h":
            response = r.get(f"{primary_node_url}/health").json()
            
        case "n":
            response = r.get(f"{primary_node_url}/nodes").json()

        case "r":
            response = {"status": "Not Implemented Yet."}

        case "c":
            response = {"status": "Not Implemented Yet."}
         
    if response:
            pprint(response, indent=2)
            print()
    else:
         print("A network error occured")
         print()

    command = input("Type in a Command: ")


