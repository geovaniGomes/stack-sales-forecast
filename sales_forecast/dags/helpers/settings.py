
from airflow.models import Variable
from minio import Minio
from sqlalchemy.engine import create_engine


class ConectionMinio:
    _instance = None
    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = Minio(
                   Variable.get("data_lake_server"),
                    access_key=Variable.get("data_lake_login"),
                    secret_key=Variable.get("data_lake_password"),
                    secure=False
                )  
        return cls._instance
