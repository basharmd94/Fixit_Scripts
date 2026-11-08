import argparse
import re
from datetime import date, datetime
from decimal import Decimal

from openpyxl import load_workbook
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


def get_item_pricing(conn, zid: int, xitem: str) -> dict:
    sql = text(
        """
        SELECT
            c.xitem,
            c.xdesc,
            c.xstdcost,
            c.xunitstk,
            COALESCE(SUM(i.xqty * i.xsign), 0) AS stock,
            COALESCE(SUM(i.xval * i.xsign), 0) AS total_value,
            CASE
                WHEN COALESCE(SUM(i.xqty * i.xsign), 0) = 0 THEN 0
                ELSE COALESCE(SUM(i.xval * i.xsign), 0) / COALESCE(SUM(i.xqty * i.xsign), 1)
            END AS avg_price
        FROM caitem c
        LEFT JOIN imtrn i ON c.xitem = i.xitem AND c.zid = i.zid
        WHERE c.zid = :zid AND c.xitem = :xitem
        GROUP BY c.xitem, c.xdesc, c.xstdcost, c.xunitstk
        """
    )
    row = conn.execute(sql, {"zid": zid, "xitem": xitem}).mappings().fetchone()
    if not row:
        raise ValueError(f"Item not found in caitem for zid={zid}: {xitem}")
    return dict(row)


def load_bulk_issue_rows(xlsx_path: str) -> list[dict]:
    wb = load_workbook(xlsx_path, data_only=True)
    ws = wb.active
    rows = []

    header = []
    for cell in next(ws.iter_rows(min_row=1, max_row=1, values_only=True)):
        header.append(str(cell).strip().lower() if cell is not None else "")

    col_map = {name: idx for idx, name in enumerate(header)}
    for required in ("zid", "sl", "item_code", "qty", "warehouse", "project"):
        if required not in col_map:
            raise ValueError(f"Missing required column '{required}' in {xlsx_path}")

    for r in ws.iter_rows(min_row=2, values_only=True):
        zid_val = r[col_map["zid"]]
        sl_val = r[col_map["sl"]]
        item_code = r[col_map["item_code"]]
        qty_val = r[col_map["qty"]]
        wh_val = r[col_map["warehouse"]]
        proj_val = r[col_map["project"]]

        if zid_val is None or str(zid_val).strip() == "":
            continue

        if item_code is None or str(item_code).strip() == "":
            continue

        if wh_val is None or str(wh_val).strip() == "":
            continue

        if proj_val is None or str(proj_val).strip() == "":
            continue

        zid = int(zid_val)
        sl = int(sl_val) if sl_val is not None and str(sl_val).strip() != "" else None
        qty = Decimal(str(qty_val or 0))
        if qty <= 0:
            continue

        rows.append(
            {
                "zid": zid,
                "xwh": str(wh_val).strip(),
                "xproj": str(proj_val).strip(),
                "xtorlno": sl,
                "xitem": str(item_code).strip(),
                "xqtyord": qty,
            }
        )

    if not rows:
        raise ValueError(f"No valid rows found in {xlsx_path}")

    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="bulk_issue.xlsx")
    parser.add_argument("--zemail", default="bashar@fixit.com")
    parser.add_argument("--xrem", default="Bulk issue upload")
    args = parser.parse_args()

    engine = get_engine()
    issue_date = date.today()
    xyear = issue_date.year
    xper = issue_date.month
    now_ts = datetime.now()

    print(f"Loading bulk issue rows from {args.file}...")
    lines = load_bulk_issue_rows(args.file)
    print(f"Loaded {len(lines)} rows from Excel file")

    print("Grouping rows by zid, warehouse, and project...")
    groups: dict[tuple[int, str, str], list[dict]] = {}
    for line in lines:
        key = (int(line["zid"]), str(line["xwh"]), str(line["xproj"]))
        groups.setdefault(key, []).append(line)
    print(f"Grouped into {len(groups)} issue groups")

    with engine.begin() as conn:

        created_numbers: list[str] = []
        group_count = 0
        for (zid, xwh, xproj), group_lines in groups.items():
            group_count += 1
            print(f"\nProcessing group {group_count}/{len(groups)}: zid={zid}, warehouse={xwh}, project={xproj} ({len(group_lines)} items)")
            group_lines_sorted = sorted(group_lines, key=lambda r: (r["xtorlno"] is None, r["xtorlno"] or 0))
            next_row = 1
            for r in group_lines_sorted:
                if r["xtorlno"] is None:
                    r["xtorlno"] = next_row
                next_row = max(next_row, int(r["xtorlno"]) + 1)

            print(f"  Generating issue number for zid={zid}...")
            iss_number = generate_voucher_number(conn, zid=zid, prefix="ISS-", table="imtemptrn", column="ximtmptrn", length=6)
            created_numbers.append(iss_number)
            print(f"  Created issue number: {iss_number}")

            print(f"  Inserting header record for {iss_number}...")
            conn.execute(
                text(
                    """
                    INSERT INTO imtemptrn (
                        ztime, zid, ximtmptrn, xdate, xyear, xper, xdatecom,
                        xwh, xproj, xsign, xaction, zemail, xtrnimf, xmember,
                        xstatustrn, xrem
                    ) VALUES (
                        :ztime, :zid, :ximtmptrn, :xdate, :xyear, :xper, :xdatecom,
                        :xwh, :xproj, :xsign, :xaction, :zemail, :xtrnimf, :xmember,
                        :xstatustrn, :xrem
                    )
                    """
                ),
                {
                    "ztime": now_ts,
                    "zid": zid,
                    "ximtmptrn": iss_number,
                    "xdate": issue_date,
                    "xyear": xyear,
                    "xper": xper,
                    "xdatecom": None,
                    "xwh": xwh,
                    "xproj": xproj,
                    "xsign": -1,
                    "xaction": "Issue",
                    "zemail": args.zemail,
                    "xtrnimf": "ISS-",
                    "xmember": args.zemail,
                    "xstatustrn": "1-Open",
                    "xrem": args.xrem,
                },
            )

            print(f"  Processing {len(group_lines_sorted)} line items...")
            for idx, line in enumerate(group_lines_sorted, 1):
                print(f"    [{idx}/{len(group_lines_sorted)}] Processing item: {line['xitem']}, qty: {line['xqtyord']}")
                print(f"      Getting pricing for item {line['xitem']}...")
                pricing = get_item_pricing(conn, zid=zid, xitem=line["xitem"])
                avg_price = Decimal(str(pricing.get("avg_price") or 0))
                xstdcost = Decimal(str(pricing.get("xstdcost") or 0))
                rate = avg_price if avg_price > 0 else xstdcost
                unit = pricing.get("xunitstk") or "Pcs"
                qty = line["xqtyord"]
                xval = qty * rate
                print(f"      Rate: {rate}, Value: {xval}, Unit: {unit}")

                print(f"      Inserting detail record for line {line['xtorlno']}...")
                conn.execute(
                    text(
                        """
                        INSERT INTO imtemptdt (
                            ztime, zid, ximtmptrn, xtorlno, xitem, xqtyord, xunit,
                            ximtrnnum, xrate, xval, zemail, xqtyreq, xdatesch,
                            xfrslnum, xtoslnum, xlineamt
                        ) VALUES (
                            :ztime, :zid, :ximtmptrn, :xtorlno, :xitem, :xqtyord, :xunit,
                            :ximtrnnum, :xrate, :xval, :zemail, :xqtyreq, :xdatesch,
                            :xfrslnum, :xtoslnum, :xlineamt
                        )
                        """
                    ),
                    {
                        "ztime": now_ts,
                        "zid": zid,
                        "ximtmptrn": iss_number,
                        "xtorlno": int(line["xtorlno"]),
                        "xitem": line["xitem"],
                        "xqtyord": qty,
                        "xunit": unit,
                        "ximtrnnum": None,
                        "xrate": rate,
                        "xval": xval,
                        "zemail": args.zemail,
                        "xqtyreq": qty,
                        "xdatesch": date(2999, 12, 31),
                        "xfrslnum": 0,
                        "xtoslnum": 0,
                        "xlineamt": xval,
                    },
                )
                print(f"      ✓ Line {line['xtorlno']} inserted successfully")

    print(f"\n{'='*60}")
    print(f"Summary: Created {len(created_numbers)} issue(s)")
    for num in created_numbers:
        print(f"  ✓ Created issue: {num}")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
