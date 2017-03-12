# SegmentBackup
For improving restarting stream processing application across data-center 

## What's in this repo
* main source files: tuple.py, node.py, pending_window.py
* application configuration: conf.yaml (trivial app which filters multiples of 8)
* some utility files

## How to run

### New start
```python start.py [-m new] [-f conf.yaml]```

### Restart
```python start.py -m restart [-f conf.yaml]```
