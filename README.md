# SQLAlchemy-and-FastAPI-with-DunnHumby
Showcase for inter-operability of SQLAlchemy/SQLModel ORM with Pydantic dataclasses and FastAPI


See the .ipynb notebooks in the /src/ directory for a bit more of a description. The .py files have the code required to host an API using uvicorn, allowing the potential of POSTing the data to a database; however, local data migration seems better in this case.  End use case is to run 'CRON'-type jobs for ETL, while allowing post/get access via the (locally-hosted) web API. 
