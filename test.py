import requests
response = requests.get(f"https://pokeapi.co/api/v2/pokemon?offset=20&limit=20")
print(response.json())