import os
import logging
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

import requests
import pymysql


# ====================== 日志设置 ======================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"goods_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ---------- 接口配置（可用环境变量覆盖） ----------
URL = os.getenv("WS_FETCH_URL", "http://psc.lzqz.cn:8880/api/v2/open/worksheet/getFilterRows")
SIGN = os.getenv("WS_SIGN", "111==")
APPKEY = os.getenv("WS_APPKEY", "222")
WORKSHEET_ID = os.getenv("WS_WORKSHEET_ID", "3333")
UPDATE_API = os.getenv("WS_UPDATE_URL", "http://psc.lzqz.cn:8880/api/v2/open/worksheet/editRows")
PAGE_SIZE = int(os.getenv("WS_PAGE_SIZE", "100"))
MAX_WORKERS = int(os.getenv("WS_MAX_WORKERS", "8"))
MAX_OUTSTANDING_PAGES = int(os.getenv("WS_MAX_OUTSTANDING_PAGES", "32"))
REQUEST_TIMEOUT = int(os.getenv("WS_REQUEST_TIMEOUT", "15"))
REQUEST_RETRIES = int(os.getenv("WS_REQUEST_RETRIES", "2"))


# ---------- MySQL（TCP连接） ----------
DB_CFG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "jdy_system"),
    "charset": "utf8mb4",
    "autocommit": True,
}

TABLE_SQL = (
    """
    CREATE TABLE IF NOT EXISTS worksheet_goods (
        rowid VARCHAR(64) PRIMARY KEY,
        skuId VARCHAR(64),
        skuName VARCHAR(255),
        skuState VARCHAR(32),
        channeStatus VARCHAR(32),
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
)

INSERT_SQL = (
    """
    INSERT INTO worksheet_goods (rowid, skuId, skuName, skuState, channeStatus, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        skuId = VALUES(skuId),
        skuName = VALUES(skuName),
        skuState = VALUES(skuState),
        channeStatus = VALUES(channeStatus),
        updated_at = VALUES(updated_at);
    """
)


def connect_db():
    return pymysql.connect(**DB_CFG)


def build_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=REQUEST_RETRIES)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_page(session: requests.Session, page_index: int) -> Tuple[int, List[Dict]]:
    payload = {
        "sign": SIGN,
        "appKey": APPKEY,
        "worksheetId": WORKSHEET_ID,
        "pageIndex": str(page_index),
        "pageSize": str(PAGE_SIZE),
    }
    try:
        resp = session.post(URL, json=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data", {}).get("rows", [])
        logger.info("📥 第 %s 页拉取到 %s 条数据", page_index, len(rows))
        return page_index, rows
    except Exception as exc:
        logger.error("❌ 第 %s 页请求失败: %s", page_index, exc)
        return page_index, []


def save_rows(rows: List[Dict]) -> int:
    if not rows:
        return 0

    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            now = datetime.now()
            payload = [
                (
                    r.get("rowid"),
                    r.get("skuId"),
                    r.get("skuName"),
                    r.get("skuState"),
                    r.get("channeStatus"),
                    now,
                )
                for r in rows
            ]
            cursor.executemany(INSERT_SQL, payload)
            logger.info("✅ 写入 %s 条记录", len(payload))
    finally:
        conn.close()

    return len(rows)


def update_uplift(session: requests.Session, rows: List[Dict]) -> None:
    if not rows:
        return

    row_ids = [r.get("rowid") for r in rows]
    skus = [r.get("skuId") for r in rows]

    data = {
        "sign": SIGN,
        "appKey": APPKEY,
        "worksheetId": WORKSHEET_ID,
        "rowIds": row_ids,
        "controls": [{"controlId": "uplift", "value": "1.08"}],
    }
    try:
        resp = session.post(UPDATE_API, json=data, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()
        if body.get("success"):
            logger.info("✅ sku%s：更新成功", skus)
        else:
            logger.error("❌ sku%s：更新失败 %s", skus, json.dumps(body, ensure_ascii=False))
    except Exception as exc:
        logger.error("❌ sku%s：更新接口异常 %s", skus, exc)


def main() -> None:
    # 建表
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute(TABLE_SQL)
    finally:
        conn.close()

    total_inserted = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        session = build_session()

        outstanding = {}
        next_page_to_submit = 1
        stop = False

        # 预填充一些页请求，形成流水线
        while len(outstanding) < min(MAX_OUTSTANDING_PAGES, MAX_WORKERS * 4) and not stop:
            fut = executor.submit(fetch_page, session, next_page_to_submit)
            outstanding[fut] = next_page_to_submit
            next_page_to_submit += 1

        while outstanding and not stop:
            for fut in as_completed(list(outstanding.keys()), timeout=None):
                page_idx, rows = fut.result()
                outstanding.pop(fut, None)

                if not rows:
                    logger.info("🚫 第 %s 页无数据，停止请求", page_idx)
                    stop = True
                    break

                total_inserted += save_rows(rows)
                update_uplift(session, rows)

                if not stop:
                    fut_new = executor.submit(fetch_page, session, next_page_to_submit)
                    outstanding[fut_new] = next_page_to_submit
                    next_page_to_submit += 1

    logger.info("🎉 所有完成，总共写入 %s 条记录", total_inserted)


if __name__ == "__main__":
    main()