"""FastAPI application. Run with `uvicorn api.main:app --reload --port 8765`."""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from google.api_core.exceptions import GoogleAPIError

from api.bigquery import MartsClient
from api.routers import market
from api.settings import Settings, get_settings

DESCRIPTION = """
Return and risk metrics for US stocks, the 11 SPDR sector ETFs and SPY, refreshed
every weekday after the US close.

Every endpoint reads the dbt marts in BigQuery (read-only, cached in memory for an
hour). Returns, volatility and drawdowns are fractions (0.25 = 25%); formatting is
left to the client. Not investment advice.
"""


def create_app(settings: Settings | None = None, marts: MartsClient | None = None) -> FastAPI:
    settings = settings or get_settings()
    app = FastAPI(title="Financial Market API", version="0.1.0", description=DESCRIPTION)
    app.state.settings = settings
    app.state.marts = marts or MartsClient(settings)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["GET"],
        allow_headers=["Content-Type"],
    )

    @app.exception_handler(GoogleAPIError)
    async def bigquery_error(request: Request, exc: GoogleAPIError) -> JSONResponse:
        return JSONResponse(status_code=502, content={"detail": "The data warehouse could not answer this request."})

    @app.get("/health", tags=["system"], summary="Liveness check")
    def health() -> dict[str, str]:
        """Returns `ok` when the API is up. Does not query BigQuery."""
        return {"status": "ok"}

    app.include_router(market.router, responses={502: {"description": "BigQuery could not answer the request."}})
    return app


app = create_app()
