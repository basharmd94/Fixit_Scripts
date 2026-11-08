import argparse
import re
from datetime import datetime
from decimal import Decimal

from sqlalchemy import text

from db import get_engine


def _extract_numeric_part(voucher_number: str, prefix: str) -> int:
    if not voucher_number or not voucher_number.startswith(prefix):
        return 0
    numeric_part = voucher_number[len(prefix):]
    digits = re.findall(r"\d+", numeric_part)
    if digits:
        return int(digits[0])
    return 0


def _format_voucher_number(prefix: str, number: int, length: int) -> str:
    return f"{prefix}{number:0{length}d}"


def generate_voucher_number(conn, zid: int, prefix: str, table: str, column: str, length: int = 6) -> str:
    sql = text(
        f"""
        SELECT {column}
        FROM {table}
        WHERE zid = :zid AND {column} LIKE :like_prefix
        ORDER BY {column} DESC
        LIMIT 1
        FOR UPDATE
        """
    )
    row = conn.execute(sql, {"zid": zid, "like_prefix": f"%{prefix}%"}).fetchone()
    if row and row[0]:
        last_number = _extract_numeric_part(str(row[0]), prefix)
        next_number = last_number + 1
    else:
        next_number = 1
    return _format_voucher_number(prefix, next_number, length)


def validate_inventory(conn, zid: int, xwh: str, items: list[dict]) -> dict:
    item_codes = [i["xitem"] for i in items]
    if not item_codes:
        return {"success": False, "message": "No items to validate", "errors": []}

    placeholders = ",".join([f":p{idx}" for idx in range(len(item_codes))])
    params = {"zid": zid, "xwh": xwh}
    for idx, code in enumerate(item_codes):
        params[f"p{idx}"] = code

    inv_sql = text(
        f"""
        SELECT xitem, SUM(xqty * xsign) AS current_stock
        FROM imtrn
        WHERE zid = :zid AND xwh = :xwh AND xitem IN ({placeholders})
        GROUP BY xitem
        """
    )
    rows = conn.execute(inv_sql, params).fetchall()
    inventory = {str(r[0]): Decimal(str(r[1] or 0)) for r in rows}

    errors = []
    for item in items:
        xitem = item["xitem"]
        requested = Decimal(str(item["quantity"] or 0))
        available = inventory.get(xitem, Decimal("0"))
        if requested > available:
            errors.append(
                {
                    "xitem": xitem,
                    "requested_quantity": float(requested),
                    "available_stock": float(available),
                }
            )

    if errors:
        return {"success": False, "message": "Inventory validation failed", "errors": errors}

    return {"success": True, "message": "Inventory validation successful", "errors": []}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("issue_number", nargs="?")
    parser.add_argument("--zid", type=int)
    parser.add_argument("--zemail", default="bashar@fixit.com")
    args = parser.parse_args()

    issue_number = args.issue_number
    if not issue_number:
        issue_number = input("Enter issue number (e.g., ISS-000012): ").strip()

    if not issue_number:
        print("Invalid issue number")
        return 1

    print(f"Processing issue confirmation for: {issue_number}")

    engine = get_engine()
    now_ts = datetime.now()

    with engine.begin() as conn:
        zid = args.zid
        if zid is None:
            print("Looking up zid from issue record...")
            zid_rows = conn.execute(
                text(
                    """
                    SELECT zid
                    FROM imtemptrn
                    WHERE ximtmptrn = :ximtmptrn
                    """
                ),
                {"ximtmptrn": issue_number},
            ).fetchall()
            zids = sorted({int(r[0]) for r in zid_rows if r and r[0] is not None})
            if not zids:
                print("Issue not found")
                return 1
            if len(zids) != 1:
                print(f"Multiple ZIDs found for {issue_number}: {', '.join(map(str, zids))}. Use --zid.")
                return 1
            zid = zids[0]
            print(f"Found zid: {zid}")
        else:
            print(f"Using provided zid: {zid}")

        print("Fetching issue header...")
        hdr = conn.execute(
            text(
                """
                SELECT ximtmptrn, xdate, xwh, xstatustrn, xproj
                FROM imtemptrn
                WHERE zid = :zid AND ximtmptrn = :ximtmptrn
                FOR UPDATE
                """
            ),
            {"zid": zid, "ximtmptrn": issue_number},
        ).fetchone()

        if not hdr:
            print("Issue not found")
            return 1

        ximtmptrn, xdate, xwh, xstatustrn, xproj = hdr
        print(f"  Issue: {ximtmptrn}, Date: {xdate}, Warehouse: {xwh}, Project: {xproj}, Status: {xstatustrn}")
        if str(xstatustrn) != "1-Open":
            print("Issue already processed")
            return 1

        print("Fetching issue detail rows...")
        detail_rows = conn.execute(
            text(
                """
                SELECT xitem, xqtyord, xrate, xval, xtorlno
                FROM imtemptdt
                WHERE zid = :zid AND ximtmptrn = :ximtmptrn
                ORDER BY xtorlno
                """
            ),
            {"zid": zid, "ximtmptrn": issue_number},
        ).fetchall()

        if not detail_rows:
            print("No items to confirm")
            return 1

        print(f"Found {len(detail_rows)} line items to process")
        items_to_check = [{"xitem": str(r[0]), "quantity": Decimal(str(r[1] or 0))} for r in detail_rows]
        print("Validating inventory...")
        validation = validate_inventory(conn, zid=zid, xwh=str(xwh), items=items_to_check)
        if not validation["success"]:
            print(validation["message"])
            for err in validation["errors"]:
                print(f"{err['xitem']}: requested={err['requested_quantity']} available={err['available_stock']}")
            return 1

        print("Inventory validation successful")

        year = xdate.strftime("%Y") if xdate else now_ts.strftime("%Y")
        month = xdate.strftime("%m") if xdate else now_ts.strftime("%m")

        print(f"\nProcessing {len(detail_rows)} line items...")
        for idx, (xitem, xqtyord, xrate, xval, xtorlno) in enumerate(detail_rows, 1):
            print(f"  [{idx}/{len(detail_rows)}] Processing line {xtorlno}: item={xitem}, qty={xqtyord}")
            qty = Decimal(str(xqtyord or 0))
            rate = Decimal(str(xrate or 0))
            line_val = Decimal(str(xval or 0)) if xval is not None else (qty * rate)

            print("    Generating voucher number...")
            im_voucher = generate_voucher_number(conn, zid=zid, prefix="ISS-", table="imtrn", column="ximtrnnum", length=6)

            print(f"    Created voucher: {im_voucher}")

            print("    Inserting into imtrn...")
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
                    "xdoctype": "ISS-",
                    "xdocnum": str(ximtmptrn),
                    "xdocrow": int(xtorlno),
                    "xaltqty": "0.000",
                    "xproj": str(xproj or ""),
                    "xdateexp": xdate,
                    "xdaterec": xdate,
                    "xaction": "Issue",
                    "xsign": -1,
                    "xtime": now_ts,
                    "zemail": args.zemail,
                    "xmember": args.zemail,
                },
            )

            print("    Updating imtemptdt with voucher number...")
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

            print(f"    ✓ Line {xtorlno} processed successfully")

        print("\nUpdating issue status to '5-Confirmed'...")
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

    print(f"\n{'='*60}")
    print(f"✓ Successfully confirmed issue: {issue_number}")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
