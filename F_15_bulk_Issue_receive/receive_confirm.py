import argparse
import os
import re
from datetime import datetime
from decimal import Decimal

from openpyxl import Workbook
from sqlalchemy import text

from db import get_engine


def _extract_numeric_part(voucher_number: str, prefix: str) -> int:
    if not voucher_number or not voucher_number.startswith(prefix):
        return 0
    numeric_part = voucher_number[len(prefix) :]
    digits = re.findall(r"\d+", numeric_part)
    if digits:
        return int(digits[0])
    return 0


def _format_voucher_number(prefix: str, number: int, length: int) -> str:
    return f"{prefix}{number:0{length}d}"


def generate_voucher_number(conn, zid: int, prefix: str, table: str, column: str, length: int = 6) -> str:
    row = conn.execute(
        text(
            f"""
            SELECT {column}
            FROM {table}
            WHERE zid = :zid AND {column} LIKE :like_prefix
            ORDER BY {column} DESC
            LIMIT 1
            FOR UPDATE
            """
        ),
        {"zid": zid, "like_prefix": f"%{prefix}%"},
    ).fetchone()

    if row and row[0]:
        last_number = _extract_numeric_part(str(row[0]), prefix)
        next_number = last_number + 1
    else:
        next_number = 1

    return _format_voucher_number(prefix, next_number, length)


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    r = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = :table
              AND column_name = :column
            LIMIT 1
            """
        ),
        {"table": table_name, "column": column_name},
    ).fetchone()
    return bool(r)


def _fetch_item_stdcosts(conn, zid: int, item_codes: list[str]) -> dict[str, Decimal]:
    unique_codes = sorted({str(c) for c in item_codes if c is not None and str(c).strip() != ""})
    if not unique_codes:
        return {}

    placeholders = ",".join([f":p{idx}" for idx in range(len(unique_codes))])
    params: dict[str, object] = {"zid": zid}
    for idx, code in enumerate(unique_codes):
        params[f"p{idx}"] = code

    rows = conn.execute(
        text(
            f"""
            SELECT xitem, xstdcost
            FROM caitem
            WHERE zid = :zid AND xitem IN ({placeholders})
            """
        ),
        params,
    ).fetchall()

    return {str(r[0]): Decimal(str(r[1] or 0)) for r in rows}


def _export_receive_rows_to_excel(rows: list[dict], receive_number: str, now_ts: datetime) -> str:
    file_name = f"receive_items_{receive_number}_{now_ts.strftime('%Y%m%d_%H%M%S')}.xlsx"
    file_path = os.path.join(os.path.dirname(__file__), file_name)

    wb = Workbook()
    ws = wb.active
    ws.title = "receive_items"

    ws.append(["xdate", "rec_number", "xdocrow", "item", "qty", "caitem_stdcost", "imtrn_val"])
    for r in rows:
        ws.append(
            [
                r.get("xdate"),
                r.get("rec_number"),
                r.get("xdocrow"),
                r.get("item"),
                r.get("qty"),
                r.get("caitem_stdcost"),
                r.get("imtrn_val"),
            ]
        )

    wb.save(file_path)
    return file_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("receive_number", nargs="?")
    parser.add_argument("--zid", type=int)
    parser.add_argument("--zemail", default="bashar@fixit.com")
    args = parser.parse_args()

    receive_number = args.receive_number
    if not receive_number:
        receive_number = input("Enter receive number (e.g., REC-000012): ").strip()

    if not receive_number:
        print("Invalid receive number")
        return 1

    print(f"Processing receive confirmation for: {receive_number}")
    engine = get_engine()
    now_ts = datetime.now()

    export_rows: list[dict] = []

    with engine.begin() as conn:
        zid = args.zid
        if zid is None:
            print("Looking up zid from receive record...")
            zid_rows = conn.execute(
                text(
                    """
                    SELECT zid
                    FROM imtemptrn
                    WHERE ximtmptrn = :ximtmptrn
                    """
                ),
                {"ximtmptrn": receive_number},
            ).fetchall()
            zids = sorted({int(r[0]) for r in zid_rows if r and r[0] is not None})
            if not zids:
                print("Receive not found")
                return 1
            if len(zids) != 1:
                print(f"Multiple ZIDs found for {receive_number}: {', '.join(map(str, zids))}. Use --zid.")
                return 1
            zid = zids[0]
            print(f"Found zid: {zid}")
        else:
            print(f"Using provided zid: {zid}")

        print("Checking database schema...")
        has_imtemptrn_xproj = _table_has_column(conn, "imtemptrn", "xproj")
        has_imtrn_xproj = _table_has_column(conn, "imtrn", "xproj")
        print(f"  imtemptrn has xproj: {has_imtemptrn_xproj}, imtrn has xproj: {has_imtrn_xproj}")

        print("Fetching receive header...")
        if has_imtemptrn_xproj:
            hdr = conn.execute(
                text(
                    """
                    SELECT ximtmptrn, xdate, xwh, xstatustrn, xproj
                    FROM imtemptrn
                    WHERE zid = :zid AND ximtmptrn = :ximtmptrn
                    FOR UPDATE
                    """
                ),
                {"zid": zid, "ximtmptrn": receive_number},
            ).fetchone()
        else:
            hdr = conn.execute(
                text(
                    """
                    SELECT ximtmptrn, xdate, xwh, xstatustrn
                    FROM imtemptrn
                    WHERE zid = :zid AND ximtmptrn = :ximtmptrn
                    FOR UPDATE
                    """
                ),
                {"zid": zid, "ximtmptrn": receive_number},
            ).fetchone()

        if not hdr:
            print("Receive not found")
            return 1

        if has_imtemptrn_xproj:
            ximtmptrn, xdate, xwh, xstatustrn, xproj = hdr
        else:
            ximtmptrn, xdate, xwh, xstatustrn = hdr
            xproj = ""

        print(f"  Receive: {ximtmptrn}, Date: {xdate}, Warehouse: {xwh}, Project: {xproj}, Status: {xstatustrn}")
        if str(xstatustrn) != "1-Open":
            print("Receive already processed")
            return 1

        print("Fetching receive detail rows...")
        detail_rows = conn.execute(
            text(
                """
                SELECT xitem, xqtyord, xrate, xval, xtorlno
                FROM imtemptdt
                WHERE zid = :zid AND ximtmptrn = :ximtmptrn
                ORDER BY xtorlno
                """
            ),
            {"zid": zid, "ximtmptrn": receive_number},
        ).fetchall()

        if not detail_rows:
            print("No items to confirm")
            return 1

        print(f"Found {len(detail_rows)} line items to process")
        year = xdate.strftime("%Y") if xdate else now_ts.strftime("%Y")
        month = xdate.strftime("%m") if xdate else now_ts.strftime("%m")

        print("Fetching standard costs...")
        stdcost_by_item = _fetch_item_stdcosts(conn, zid=zid, item_codes=[str(r[0]) for r in detail_rows])
        print(f"  Loaded standard costs for {len(stdcost_by_item)} item(s)")

        print(f"\nProcessing {len(detail_rows)} line items...")
        for idx, (xitem, xqtyord, xrate, xval, xtorlno) in enumerate(detail_rows, 1):
            print(f"  [{idx}/{len(detail_rows)}] Processing line {xtorlno}: item={xitem}, qty={xqtyord}")
            qty = Decimal(str(xqtyord or 0))
            rate = Decimal(str(xrate or 0))
            line_val = Decimal(str(xval or 0)) if xval is not None else (qty * rate)

            print(f"    Generating voucher number...")
            im_voucher = generate_voucher_number(conn, zid=zid, prefix="REC-", table="imtrn", column="ximtrnnum", length=6)
            print(f"    Created voucher: {im_voucher}")

            print(f"    Inserting into imtrn...")
            if has_imtrn_xproj:
                conn.execute(
                    text(
                        """
                        INSERT INTO imtrn (
                            ztime, zid, ximtrnnum, xitem, xwh, xdate, xyear, xper,
                            xqty, xval, xdoctype, xdocnum, xdocrow, xaltqty,
                            xproj, xdateexp, xdaterec, xaction, xsign, xtime, zemail, xmember
                        ) VALUES (
                            :ztime, :zid, :ximtrnnum, :xitem, :xwh, :xdate, :xyear, :xper,
                            :xqty, :xval, :xdoctype, :xdocnum, :xdocrow, :xaltqty,
                            :xproj, :xdateexp, :xdaterec, :xaction, :xsign, :xtime, :zemail, :xmember
                        )
                        """
                    ),
                    {
                        "ztime": now_ts,
                        "zid": zid,
                        "ximtrnnum": im_voucher,
                        "xitem": str(xitem),
                        "xwh": str(xwh),
                        "xdate": xdate,
                        "xyear": str(year),
                        "xper": str(month),
                        "xqty": f"{float(qty):.3f}",
                        "xval": f"{float(line_val):.6f}",
                        "xdoctype": "REC-",
                        "xdocnum": str(ximtmptrn),
                        "xdocrow": int(xtorlno),
                        "xaltqty": "0.000",
                        "xproj": str(xproj or ""),
                        "xdateexp": xdate,
                        "xdaterec": xdate,
                        "xaction": "Receipt",
                        "xsign": 1,
                        "xtime": now_ts,
                        "zemail": args.zemail,
                        "xmember": args.zemail,
                    },
                )
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO imtrn (
                            ztime, zid, ximtrnnum, xitem, xwh, xdate, xyear, xper,
                            xqty, xval, xdoctype, xdocnum, xdocrow, xaltqty,
                            xdateexp, xdaterec, xaction, xsign, xtime, zemail, xmember
                        ) VALUES (
                            :ztime, :zid, :ximtrnnum, :xitem, :xwh, :xdate, :xyear, :xper,
                            :xqty, :xval, :xdoctype, :xdocnum, :xdocrow, :xaltqty,
                            :xdateexp, :xdaterec, :xaction, :xsign, :xtime, :zemail, :xmember
                        )
                        """
                    ),
                    {
                        "ztime": now_ts,
                        "zid": zid,
                        "ximtrnnum": im_voucher,
                        "xitem": str(xitem),
                        "xwh": str(xwh),
                        "xdate": xdate,
                        "xyear": str(year),
                        "xper": str(month),
                        "xqty": f"{float(qty):.3f}",
                        "xval": f"{float(line_val):.6f}",
                        "xdoctype": "REC-",
                        "xdocnum": str(ximtmptrn),
                        "xdocrow": int(xtorlno),
                        "xaltqty": "0.000",
                        "xdateexp": xdate,
                        "xdaterec": xdate,
                        "xaction": "Receipt",
                        "xsign": 1,
                        "xtime": now_ts,
                        "zemail": args.zemail,
                        "xmember": args.zemail,
                    },
                )

            print(f"    Updating imtemptdt with voucher number...")
            conn.execute(
                text(
                    """
                    UPDATE imtemptdt
                    SET ximtrnnum = :ximtrnnum
                    WHERE zid = :zid AND ximtmptrn = :ximtmptrn AND xtorlno = :xtorlno
                    """
                ),
                {"ximtrnnum": im_voucher, "zid": zid, "ximtmptrn": str(ximtmptrn), "xtorlno": int(xtorlno)},
            )

            export_rows.append(
                {
                    "xdate": xdate,
                    "rec_number": str(ximtmptrn),
                    "xdocrow": int(xtorlno),
                    "item": str(xitem),
                    "qty": float(qty),
                    "caitem_stdcost": float(stdcost_by_item.get(str(xitem), Decimal("0"))),
                    "imtrn_val": float(line_val),
                }
            )
            print(f"    ✓ Line {xtorlno} processed successfully")

        print("\nUpdating receive status to '5-Confirmed'...")
        conn.execute(
            text(
                """
                UPDATE imtemptrn
                SET xstatustrn = '5-Confirmed', zutime = :zutime, xdatecom = :xdatecom
                WHERE zid = :zid AND ximtmptrn = :ximtmptrn
                """
            ),
            {"zutime": now_ts, "xdatecom": xdate, "zid": zid, "ximtmptrn": str(ximtmptrn)},
        )

    if export_rows:
        print("\nExporting inserted items to Excel...")
        export_path = _export_receive_rows_to_excel(export_rows, receive_number=str(receive_number), now_ts=now_ts)
        print(f"✓ Exported: {export_path}")

    print(f"\n{'='*60}")
    print(f"✓ Successfully confirmed receive: {receive_number}")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
