import os
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple

import requests
import pymysql

# Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "test"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.Cursor,
    "autocommit": True,
}

PRICE_UPDATE_API = os.getenv("PRICE_UPDATE_API", "http://localhost:8000/price/update")
STATE_UPDATE_API = os.getenv("STATE_UPDATE_API", "http://localhost:8000/state/update")

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
REQUEST_RETRY_TIMES = int(os.getenv("REQUEST_RETRY_TIMES", "2"))
REQUEST_RETRY_BACKOFF_SECONDS = float(os.getenv("REQUEST_RETRY_BACKOFF_SECONDS", "1.5"))

lock = threading.Lock()


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
    )


def connect_db():
    return pymysql.connect(**DB_CONFIG)


def get_all_goods_ids() -> List[int]:
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT goodsId FROM b2b_goods_result GROUP BY goodsId")
            goods_ids = [row[0] for row in cursor.fetchall()]
        return goods_ids
    finally:
        conn.close()


def get_skus_by_goods_id(goods_id: int) -> List[Dict]:
    conn = connect_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(
                """
                SELECT skuId, costPrice, salePrice, state
                FROM b2b_goods_result
                WHERE goodsId = %s
                """,
                (goods_id,),
            )
            rows = cursor.fetchall()
        return rows
    finally:
        conn.close()


def insert_failure(goods_id: int, sku_list: List[Dict], error_msg: str) -> None:
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            sql = (
                """
                INSERT INTO price_update_failures (goodsId, sku_list_json, error_msg)
                VALUES (%s, %s, %s)
                """
            )
            sku_list_json = json.dumps(sku_list, ensure_ascii=False)
            cursor.execute(sql, (goods_id, sku_list_json, error_msg[:500]))
    finally:
        conn.close()


def insert_process_log(
    goods_id: int,
    price_status: str,
    state_status: str,
    price_msg: str | None = None,
    state_msg: str | None = None,
) -> None:
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            sql = (
                """
                INSERT INTO b2b_process_log (goodsId, price_status, state_status, price_msg, state_msg)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    price_status=VALUES(price_status), 
                    state_status=VALUES(state_status),
                    price_msg=VALUES(price_msg),
                    state_msg=VALUES(state_msg)
                """
            )
            cursor.execute(sql, (goods_id, price_status, state_status, price_msg, state_msg))
    finally:
        conn.close()


def _request_with_retries(url: str, payload: Dict) -> Tuple[str, str]:
    last_error = ""
    for attempt in range(1, REQUEST_RETRY_TIMES + 2):
        try:
            resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            return "success", ""
        except Exception as exc:
            last_error = str(exc)
            logging.warning("Request failed (attempt %s/%s) url=%s error=%s", attempt, REQUEST_RETRY_TIMES + 1, url, last_error)
            if attempt <= REQUEST_RETRY_TIMES:
                time.sleep(REQUEST_RETRY_BACKOFF_SECONDS * attempt)
    return "fail", last_error


def price_update_api(goods_id: int, sku_list: List[Dict]) -> Tuple[str, str]:
    status, msg = _request_with_retries(PRICE_UPDATE_API, {"goodsId": goods_id, "skuList": sku_list})
    if status == "success":
        logging.info("价格更新成功 goodsId=%s", goods_id)
    else:
        logging.error("价格更新失败 goodsId=%s msg=%s", goods_id, msg)
    return status, msg


def state_update_api(goods_id: int, sku_list: List[Dict]) -> Tuple[str, str]:
    status, msg = _request_with_retries(STATE_UPDATE_API, {"goodsId": goods_id, "skuList": sku_list})
    if status == "success":
        logging.info("上下架状态更新成功 goodsId=%s", goods_id)
    else:
        logging.error("上下架状态更新失败 goodsId=%s msg=%s", goods_id, msg)
    return status, msg


def process_goods_id(goods_id: int) -> None:
    try:
        sku_rows = get_skus_by_goods_id(goods_id)
        if not sku_rows:
            logging.warning("商品 %s 没有 SKU，跳过", goods_id)
            with lock:
                insert_process_log(goods_id, "skip", "skip", "no sku", "no sku")
            return

        price_list: List[Dict] = []
        for row in sku_rows:
            if row["costPrice"] is not None and row["salePrice"] is not None:
                price_list.append(
                    {
                        "skuId": row["skuId"],
                        "costPrice": float(row["costPrice"]),
                        "salePrice": float(row["salePrice"]),
                    }
                )

        price_status, price_msg = ("skip", "no price to update")
        if price_list:
            price_status, price_msg = price_update_api(goods_id, price_list)
            if price_status == "fail":
                with lock:
                    insert_failure(goods_id, price_list, price_msg)

        state_list: List[Dict] = []
        for row in sku_rows:
            if row["costPrice"] is None or row["state"] in ("下架", None, ""):
                state_list.append({"skuId": row["skuId"], "stockNum": "0"})

        state_status, state_msg = ("skip", "no state to update")
        if state_list:
            state_status, state_msg = state_update_api(goods_id, state_list)
            if state_status == "fail":
                with lock:
                    insert_failure(goods_id, state_list, state_msg)

        with lock:
            insert_process_log(goods_id, price_status, state_status, price_msg, state_msg)

    except Exception as exc:
        logging.exception("处理 goodsId=%s 时发生异常: %s", goods_id, exc)
        with lock:
            try:
                insert_failure(goods_id, [], f"unhandled: {exc}")
                insert_process_log(goods_id, "fail", "fail", str(exc), str(exc))
            except Exception:
                logging.exception("记录异常失败 goodsId=%s", goods_id)



def main() -> None:
    configure_logging()
    goods_ids = get_all_goods_ids()
    logging.info("共查询到 %s 个商品ID", len(goods_ids))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_goods_id, gid) for gid in goods_ids]
        for future in as_completed(futures):
            future.result()

    logging.info("所有商品更新任务完成")


if __name__ == "__main__":
    main()