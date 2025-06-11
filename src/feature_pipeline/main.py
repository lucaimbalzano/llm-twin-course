import bytewax.operators as op
from bytewax.dataflow import Dataflow
from core.db.qdrant import QdrantDatabaseConnector
from data_flow.stream_input import RabbitMQSource
from data_flow.stream_output import QdrantOutput
from data_logic.dispatchers import (
    ChunkingDispatcher,
    CleaningDispatcher,
    EmbeddingDispatcher,
    RawDispatcher,
)

connection = QdrantDatabaseConnector()

# 1) Create a Dataflow
flow = Dataflow("Streaming ingestion pipeline")

# 2) Read from RabbitMQ
stream = op.input("input", flow, RabbitMQSource())

# 3) Process the Data
stream = op.map("raw dispatch", stream, RawDispatcher.handle_mq_message)
stream = op.map("clean dispatch", stream, CleaningDispatcher.dispatch_cleaner)

# 4) Qdrant upsertion inside QdrantOutput build()
op.output(
    "cleaned data insert to qdrant",
    stream,
    QdrantOutput(connection=connection, sink_type="clean"),
)

# 5) Process the Data
stream = op.flat_map("chunk dispatch", stream, ChunkingDispatcher.dispatch_chunker)
stream = op.map(
    "embedded chunk dispatch", stream, EmbeddingDispatcher.dispatch_embedder
)

# 6) Upsertion (?)
op.output(
    "embedded data insert to qdrant",
    stream,
    QdrantOutput(connection=connection, sink_type="vector"),
)