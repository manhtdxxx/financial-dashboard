import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load .env file
current_path = os.path.dirname(__file__)
env_path = os.path.join(current_path, "..", "..", ".env")
load_dotenv(dotenv_path=env_path)

# Get variables from .env file
def get_env_variable(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise EnvironmentError(f"Environment variable '{name}' is not set.")
    return value


# create engine to connect to mysql
def create_mysql_engine(user_key, password_key, host_key, port_key, db_key):
    user = get_env_variable(user_key)
    password = get_env_variable(password_key)
    host = get_env_variable(host_key)
    port = get_env_variable(port_key)
    db = get_env_variable(db_key)

    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")


def get_source_mysql_engine():
    return create_mysql_engine(
        "SOURCE_MYSQL_USER",
        "SOURCE_MYSQL_PASSWORD",
        "SOURCE_MYSQL_HOST",
        "SOURCE_MYSQL_PORT",
        "SOURCE_MYSQL_DB"
    )


def get_target_mysql_engine():
    return create_mysql_engine(
        "TARGET_MYSQL_USER",
        "TARGET_MYSQL_PASSWORD",
        "TARGET_MYSQL_HOST",
        "TARGET_MYSQL_PORT",
        "TARGET_MYSQL_DB"
    )