"""HTTP API for product search and question answering.

    uvicorn retail.api.main:app --port 8000      (or: make api; docs at /docs)

Everything runs as the read-only retail_reader role. Endpoints are plain `def`,
so FastAPI runs them on its thread pool (the database calls are blocking).
"""

from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, Query, Request

from retail import config
from retail.qa.engine import QueryEngine
from retail.qa.parse import StoreRef
from retail.qa.service import AskRequest, AskResponse, MatchOut, QAService, match_out
from retail.search.embedder import Embedder
from retail.search.resolve import ProductResolver


def build_service(reader_dsn: str = config.POSTGRES_READER_DSN,
                  lake_dir=config.LAKE_DIR) -> QAService:
    with psycopg.connect(reader_dsn) as conn:
        stores = [StoreRef(*r) for r in conn.execute(
            "SELECT store_id, name, city FROM stores ORDER BY store_id")]
    resolver = ProductResolver(reader_dsn, Embedder())
    return QAService(resolver, QueryEngine(lake_dir, reader_dsn), stores)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the embedding model and open DuckDB once, not per request.
    if not hasattr(app.state, "qa"):  # tests inject their own
        app.state.qa = build_service()
    yield


app = FastAPI(
    title="Retail Data Platform API",
    description="Hybrid product search and template-based question answering over the "
                "live store (Postgres) and sales history (Delta Lake).",
    version="0.5.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/search", response_model=MatchOut)
def search(request: Request, q: str = Query(min_length=2, max_length=100)) -> MatchOut:
    """Resolve free text to products: category, brand, name, or semantic match."""
    return match_out(request.app.state.qa.resolver.resolve(q))


@app.post("/ask", response_model=AskResponse)
def ask(request: Request, body: AskRequest) -> AskResponse:
    """Answer a question about sales, top sellers, stock or prices, with the SQL used."""
    return request.app.state.qa.ask(body.question, body.product_ids)
