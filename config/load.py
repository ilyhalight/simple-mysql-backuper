import re
import os
import toml
from dotenv import load_dotenv


def load_cfg(file: str):
    """Загружает выбранный cfg файл из папки с конфигами

    Args:
        file (str): Файл, который нужно загрузить

    Returns:
        toml: Загруженный toml файл
    """
    if re.compile(".*.cfg").fullmatch(file):
        cfg = toml.load('./config/' + file)
    else:
        cfg = False
    return cfg

def load_env():
    """Загружает .env файл"""
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')

    if os.path.exists(dotenv_path):
        load_dotenv('./config/.env')
        return True
    return False