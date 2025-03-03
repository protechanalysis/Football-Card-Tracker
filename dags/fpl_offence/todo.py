import json
import requests

r = requests.get('http://httpbin.org/stream/20', stream=True)

lines = r.iter_lines()
# Save the first line for later or just skip it

first_line = next(lines)

for line in lines:
    if line:
        print(json.loads(line))
