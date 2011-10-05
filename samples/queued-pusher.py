from soften.batcher import BatchToQueuePusher
from soften.samples.api import movies_api

queue_name = "batch_updates"

BatchToQueuePusher(
    queue_name,
    'localhost',
    11300,
    api = movies_api,
    starts_from = 0,
    chunk_size = 300
).run()



