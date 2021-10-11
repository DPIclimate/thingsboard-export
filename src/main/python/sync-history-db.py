import json
import logging
import mysql.connector
import os

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

with open("config.json", "r") as configFile:
    config = json.load(configFile)

history_config = config["mysql"]
fdt_broker_config = config["fdtBroker"]


def get_last_rawdata_uid(db_cfg: dict) -> int:
    """
    Find the highest UID from the RawData table in the database described by db_cfg.

    Args:
        db_cfg: a database configuration section from the config.json file.

    Returns:
        the highest UID from the RawData table in the database described by db_cfg.
    """

    with mysql.connector.connect(host=db_cfg['host'], port=db_cfg['port'], user=db_cfg['user'],
                                 password=db_cfg['password'], database=db_cfg['database']) as all_msgs_conn:
        with all_msgs_conn.cursor(buffered=True) as all_msgs_cursor:
            all_msgs_cursor.execute("select max(uid) from RawData")
            rs = all_msgs_cursor.fetchone()
            return rs[0]


def process_batch(db_cfg: dict, start_uid: int) -> dict:
    """

    Args:
        db_cfg:
        start_uid:

    Returns:

    """

    row_count = 0
    max_uid = start_uid
    with mysql.connector.connect(host=db_cfg['host'], port=db_cfg['port'], user=db_cfg['user'],
                                 password=db_cfg['password'], database=db_cfg['database']) as all_msgs_conn:
        with all_msgs_conn.cursor(buffered=True) as all_msgs_cursor:
            all_msgs_cursor.execute("select uid, payload, state from RawData where uid > %s order by uid limit 10", (start_uid, ))
            for (uid, payload, state) in all_msgs_cursor.fetchall():
                max_uid = uid
                row_count += 1
                print(uid, state, payload[:180])

    return {"maxUid": max_uid, "rowCount": row_count}


def main():
    os.chdir("/tmp")

    broker_uid = get_last_rawdata_uid(fdt_broker_config)
    backup_uid = get_last_rawdata_uid(history_config)

    log.info(f"Broker max uid: {broker_uid}")
    log.info(f"Backup max uid: {backup_uid}")

    if broker_uid <= backup_uid:
        log.info("History version of RawData is up-to-date.")
        return
"""
    row_count = 0
    start_uid = backup_uid
    while row_count < 99:
        rc = process_batch(fdt_broker_config, start_uid)
        start_uid = rc["maxUid"]
        row_count += rc["rowCount"]
"""

if __name__ == '__main__':
    main()
