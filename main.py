# main.py
import uvicorn
from fastapi import FastAPI

from app.api.exception_handlers.exception_handlers import register_exception_handlers
from app.api.tiles_routes import tiles_router
from app.api.images_routes import ingest_router, storage_router, meta_router, images_list_router
from fastapi.responses import JSONResponse
from starlette.responses import HTMLResponse
from fastapi import Request

from config import settings


class APITile:
    def __init__(self):

        self.app = FastAPI(title="TILE PR API", description="API для ...", version="1.0.0") #docs_url=None
        register_exception_handlers(self.app)

        self.register_routes()

    def register_routes(self):
        self.app.include_router(tiles_router)
        self.app.include_router(ingest_router)
        self.app.include_router(storage_router)
        self.app.include_router(meta_router)
        self.app.include_router(images_list_router)

        @self.app.get("/", response_class=HTMLResponse)
        async def base_page(request: Request):
            return "<html><body><h1>Добро пожаловать в сервис METADATA_SERVICE!</h1></body></html>"

        @self.app.get("/health")
        def health():
            return JSONResponse(content={"status": "ok"}, status_code=200)

    def run(self, host, port):
        uvicorn.run(self.app, host=host, port=port)

server = APITile()

if __name__ == "__main__":
    server.run(host=settings.UVICORN_SERVER_HOST, port=settings.UVICORN_SERVER_PORT)

