import json
import logging

import mysql.connector

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

with open("config.json", "r") as configFile:
    config = json.load(configFile)

history_config = config["mysql"]
fdt_broker_config = config["fdtBroker"]


def get_mysql_connection(db_cfg: dict):
    return mysql.connector.connect(host=db_cfg['host'], port=db_cfg['port'], user=db_cfg['user'],
                                   password=db_cfg['password'], database=db_cfg['database'])


fdt_connection = get_mysql_connection(fdt_broker_config)
history_connection = get_mysql_connection(history_config)


# Temporarily passing in table name so we can use a different table name in the history db while developing.
def get_last_rawdata_uid(connection, table_name) -> int:
    """
    Find the highest UID from the RawData table in the database described by db_cfg.

    Args:
        db_cfg: a database configuration section from the config.json file.

    Returns:
        the highest UID from the RawData table in the database described by db_cfg.
    """
    with connection.cursor(buffered=True) as all_msgs_cursor:
        all_msgs_cursor.execute(f"select max(uid) from {table_name}")
        rs = all_msgs_cursor.fetchone()
        return rs[0]


def process_batch(start_uid: int) -> dict:
    with fdt_connection.cursor(buffered=True) as fdt_cursor:
        fdt_cursor.execute("select uid, payload, state from RawData where uid > %s order by uid limit 2000", (start_uid, ))
        row_list = fdt_cursor.fetchall()
        row_count = len(row_list)
        last_row = row_list[-1]

        with history_connection.cursor() as history_cursor:
            history_cursor.executemany("insert into RawDataTest(uid, payload, state) values (%s, %s, %s)", row_list)
            history_connection.commit()

    return {"maxUid": last_row[0], "rowCount": row_count}


def main():
    #os.chdir("/tmp")

    broker_uid = get_last_rawdata_uid(fdt_connection, "RawData")
    history_uid = get_last_rawdata_uid(history_connection, "RawDataTest")

    log.info(f"Broker max uid: {broker_uid}")
    log.info(f"History max uid: {history_uid}")
    log.info(f"Msgs to Sync: {broker_uid - history_uid}")

    if broker_uid <= history_uid:
        log.info("History version of RawData is up-to-date, skipping sync.")
        return
    else:
        while history_uid < broker_uid:
            log.info("Starting batch")
            curr_batch = process_batch(history_uid)
            log.info(f"Processed {curr_batch['rowCount']} rows, up to Uid: {curr_batch['maxUid']}")
            history_uid = get_last_rawdata_uid(history_connection, "RawDataTest")
            log.info(f"Msgs left to Sync: {broker_uid - history_uid}")


if __name__ == '__main__':
    main()
    fdt_connection.close()
    history_connection.close()
