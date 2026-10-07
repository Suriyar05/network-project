from fastapi import FastAPI
from routes.network import network_router
from database import Base, engine
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


Base.metadata.create_all(
    bind=engine
)

app = FastAPI(
    title="Network Intelligence Service",
    version="1.0.0"
)

origins = [
"http://localhost:5173",
]

# Adding CORSMiddleware to the FastAPI application
app.add_middleware(
CORSMiddleware,
allow_origins=origins, # List of allowed origins
allow_credentials=True, # Allow credentials such as cookies and authorization headers
allow_methods=["*"], # Allow all HTTP methods
allow_headers=["*"], # Allow all HTTP headers
)
app.include_router(
    router=network_router
)