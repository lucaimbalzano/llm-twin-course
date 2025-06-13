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

# 2) Read from RabbitMQ Articles, Post, Messages
stream = op.input("input", flow, RabbitMQSource())

# 3) Process the Data
#     handle_mq_message() -> Map JSON to Pydantic Model 
stream = op.map("raw dispatch", stream, RawDispatcher.handle_mq_message)
#     dispatch_cleaner() -> Apply data-specific cleaning
stream = op.map("clean dispatch", stream, CleaningDispatcher.dispatch_cleaner)

# 4) Qdrant upsertion inside QdrantOutput build() --> for fine-tuning
op.output(
    "cleaned data insert to qdrant",
    stream,
    QdrantOutput(connection=connection, sink_type="clean"),
)

# 5) Process the Data
#     dispatch_chunker() -> Chunk the data & Flatten the results into 1D list
stream = op.flat_map("chunk dispatch", stream, ChunkingDispatcher.dispatch_chunker)
#     dispatch_embedder() -> embed chunks
stream = op.map(
    "embedded chunk dispatch", stream, EmbeddingDispatcher.dispatch_embedder
)

# 6) Qdrant upsertion inside QdrantOutput build() --> for RAG
op.output(
    "embedded data insert to qdrant",
    stream,
    QdrantOutput(connection=connection, sink_type="vector"),
)