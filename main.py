import os
import csv
import aiomysql
import asyncio
import logging

from datetime import datetime
from config.load import load_env, load_cfg
from logger import init_logging

log = logging.getLogger('backuper')
settings = load_cfg('settings.cfg')['main']
BACKUP_DIR = 'backups'
TODAY_NOW = datetime.now().strftime("%d.%m.%Y %H.%M.%S")

def create_backup_dir():
    if not os.path.isdir(BACKUP_DIR):
        try:
            os.mkdir(BACKUP_DIR)
        except Exception as err:
            log.exception(err)
            return False

    if not os.path.isdir(f'{BACKUP_DIR}/{TODAY_NOW}'):
        try:
            os.mkdir(f'{BACKUP_DIR}/{TODAY_NOW}')
        except Exception as err:
            log.exception(err)
            return False
    return True

async def connect():
    try:
        db = await aiomysql.connect(host = os.environ.get('DB_HOST'),
                                    user = os.environ.get('DB_USER'),
                                    password = os.environ.get('DB_PASS'),
                                    auth_plugin = 'mysql_native_password',
                                    charset = 'utf8',
                                    connect_timeout = 30,
                                    cursorclass = aiomysql.DictCursor
                                )
        return db
    except aiomysql.OperationalError as err:
        log.error('Failed to connect to database \"Can`t connect to MySQL Server\"')
        return False
    except Exception as err:
        log.error('Failed to connect to database \"Raw reason\"')
        log.exception(f'Failed to connect to database: {err}')
        return None

async def get_databases():
    db = await connect()
    databases = []
    async with db.cursor() as cursor:
        await cursor.execute('SHOW DATABASES')
        result = await cursor.fetchall()
        for res in result:
            if res['Database'] not in ['test', 'mysql', 'sys', 'performance_schema', 'information_schema', 'phpmyadmin']:
                databases.append(res['Database'])
    log.debug(f'Availabled databases: {databases}')
    return databases

async def make_backup(databases):
    db = await connect()
    create_backup_dir()
    log.info('Creating backup...')
    async with db.cursor() as cursor:
        for database in databases:
            if database not in settings['ignore_dbs']:
                try:
                    await cursor.execute(f'SHOW TABLES FROM {database}')
                    tables = await cursor.fetchall()
                    log.info(f'Creating a {database} database backup...')
                    for table in tables:
                        await cursor.execute(f'USE {database}')
                        await cursor.execute(f'SELECT * FROM {table[f"Tables_in_{database}"]}')
                        result = await cursor.fetchall()
                        if len(result) > 0 and type(result) is not tuple:
                            with open(f'{BACKUP_DIR}/{TODAY_NOW}/{database}.{table[f"Tables_in_{database}"]}.csv', 'w', newline = '', encoding='utf-8') as csvfile:
                                fieldnames = list(result[0].keys())
                                writer = csv.DictWriter(csvfile, fieldnames = fieldnames, quotechar = '"', quoting = csv.QUOTE_ALL, delimiter = ';')
                                writer.writeheader()
                                for res in result:
                                    writer.writerow(res)
                except Exception as err:
                    pass
    log.info(f'Backup successfully created. Check the backup directory: "{BACKUP_DIR}/{TODAY_NOW}"')


if __name__ == '__main__':
    init_logging()
    load_env()
    db = asyncio.run(get_databases())
    asyncio.run(make_backup(db))
