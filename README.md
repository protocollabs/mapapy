
## Usage

```
# start server in background
./maparo-pulser.py --daemon

# start client and pipe stats to analyzer:
./maparo-pulser.py -f templates/5-streams-10-byte-random.conf --ctrl-addr ff02::1 | ./toolbox/pulse-grapher.py
```
