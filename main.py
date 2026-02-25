# main.py


from __future__ import annotations

import uvicorn
from fastapi import FastAPI
from app.api.tiles_routes import tiles_router
from app.api.images_routes import ingest_router, storage_router, meta_router
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.docs import (
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)

from fastapi.responses import JSONResponse
from starlette.responses import HTMLResponse
from fastapi import Request

from config import settings


class APITile:
    def __init__(self):

        self.app = FastAPI(title="TILE PR API", description="API для ...", version="1.0.0") #docs_url=None

        # self.app.add_middleware(BasicAuthMiddleware)
        # self.app.mount("/static", StaticFiles(directory="static"), name="static")
        self.register_routes()

    def register_routes(self):
        self.app.include_router(tiles_router)
        self.app.include_router(ingest_router)
        self.app.include_router(storage_router)
        self.app.include_router(meta_router)

        @self.app.get("/", response_class=HTMLResponse)
        async def base_page(request: Request):
            return "<html><body><h1>Добро пожаловать в сервис METADATA_SERVICE!</h1></body></html>"

        @self.app.get("/health")
        def health():
            return JSONResponse(content={"status": "ok"}, status_code=200)

        # @self.app.get("/docs", include_in_schema=False)
        # async def custom_swagger_ui_html():
        #     return get_swagger_ui_html(
        #         openapi_url=self.app.openapi_url,
        #         title="APITile",
        #         oauth2_redirect_url=self.app.swagger_ui_oauth2_redirect_url,
        #         swagger_js_url="/static/swagger/js/swagger-ui-bundle.js",
        #         swagger_css_url="/static/swagger/css/swagger-ui.css",
        #     )
        #
        # @self.app.get(self.app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
        # async def swagger_ui_redirect():
        #     return get_swagger_ui_oauth2_redirect_html()

    def run(self, host, port):
        uvicorn.run(self.app, host=host, port=port)

server = APITile()

if __name__ == "__main__":
    server.run(host=settings.UVICORN_SERVER_HOST, port=settings.UVICORN_SERVER_PORT)

