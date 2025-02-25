```bash
# Terminal 1 - Infrastructure
cd redpanda && docker compose up

# Terminal 2 - Consumer
resonate-redpanda consume

# Terminal 3 - Producer
resonate-redpanda produce -n 10
```
