# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "33294ffb-1bfb-4a4a-acb3-e9e607695d04",
# META       "default_lakehouse_name": "bronze_lake",
# META       "default_lakehouse_workspace_id": "14524749-a634-4345-bccf-4407b30d1ca9",
# META       "known_lakehouses": [
# META         {
# META           "id": "33294ffb-1bfb-4a4a-acb3-e9e607695d04"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

"""
Import the AdventureWorks sample databases into a Microsoft Fabric Lakehouse
=============================================================================

Loads Microsoft's official AdventureWorks sample data (the same CSV files that
the SQL Server install scripts bulk-load) into Delta tables in a Lakehouse.

    * AdventureWorks    (OLTP)            -> 68 tables  (Person, Sales, Production, ...)
    * AdventureWorksDW  (data warehouse)  -> 29 tables  (DimCustomer, FactInternetSales, ...)

No SQL Server is needed. The script downloads the release zips from
github.com/microsoft/sql-server-samples, reads the table definitions and the
BULK INSERT options straight out of the install scripts (instawdb.sql /
instawdbdw.sql), parses each data file with the right encoding and delimiters,
and writes typed Delta tables.

How to run (Fabric)
-------------------
1. Create or open a Lakehouse, then create a Notebook and attach the Lakehouse
   as its default Lakehouse.
2. Paste this file into a cell (or upload it to the Lakehouse's Files area and
   `%run` it), adjust the CONFIG block below if you like, and run.
3. The tables appear under the Lakehouse's Tables section.

It also runs on Databricks or any Spark 3.3+ cluster with Delta Lake. Set
RAW_DIR to a local/driver path there.

Type mapping
------------
    int/bigint/smallint/tinyint -> int/long/short/short
    bit (incl. Flag, NameStyle) -> boolean
    decimal/numeric(p,s)        -> decimal(p,s)
    money / smallmoney          -> decimal(19,4) / decimal(10,4)
    float / real                -> double / float
    date                        -> date
    datetime/datetime2/smalldt  -> timestamp
    varbinary/binary/image      -> binary
    geography, hierarchyid      -> string (hex, as shipped in the sample files)
    xml, uniqueidentifier, text -> string
User-defined types (Name, Phone, AccountNumber, ...) are resolved to their
base type. Computed columns (e.g. SalesOrderHeader.SalesOrderNumber,
SalesOrderDetail.LineTotal) are recreated with the same expressions, so the
tables match what you'd see in SQL Server.
"""

# ============================== CONFIG =======================================
DATASETS = ["oltp", "dw"]  # "oltp" = AdventureWorks, "dw" = AdventureWorksDW

# Table naming. With USE_LAKEHOUSE_SCHEMAS = False, tables are flat:
#   oltp: aw_person_address, aw_sales_salesorderheader, ...
#   dw:   awdw_dimcustomer, awdw_factinternetsales, ...
# With True (only for schema-enabled Lakehouses), they're written as
#   <schema_prefix>_<sqlschema>.<table>, e.g. aw_person.address, awdw_dbo.dimcustomer
USE_LAKEHOUSE_SCHEMAS = False
TABLE_PREFIX = {"oltp": "aw", "dw": "awdw"}
LOWERCASE_NAMES = True  # Keep table names lower-case (column names keep their casing)

WRITE_MODE = "overwrite"  # "overwrite" makes the script safe to re-run
TABLE_FORMAT = "delta"
ONLY_TABLES = []  # e.g. ["Sales.SalesOrderHeader", "dbo.DimDate"]; empty = all

# Where the downloaded zips and extracted files are kept. In Fabric the default
# Lakehouse's Files area is mounted at /lakehouse/default/Files.
RAW_DIR = "/lakehouse/default/Files/adventureworks_raw"

SOURCES = {
    "oltp": {
        "zip": "https://github.com/microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks-oltp-install-script.zip",
        "sql": "instawdb.sql",
    },
    "dw": {
        "zip": "https://github.com/microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksDW-data-warehouse-install-script.zip",
        "sql": "instawdbdw.sql",
    },
}
# =============================================================================

import io
import os
import re
import time
import zipfile
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ------------------------------------------------------------------------------
# 1. Download
# ------------------------------------------------------------------------------
def download_and_extract(dataset: str, raw_dir: str) -> str:
    """Download the release zip (once) and extract it. Returns the folder path."""
    target = os.path.join(raw_dir, dataset)
    sql_name = SOURCES[dataset]["sql"]
    if os.path.exists(os.path.join(target, sql_name)):
        print(f"[{dataset}] using cached files in {target}")
        return target
    os.makedirs(target, exist_ok=True)
    url = SOURCES[dataset]["zip"]
    print(f"[{dataset}] downloading {url}")
    with urllib.request.urlopen(url, timeout=300) as resp:
        payload = resp.read()
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        for member in zf.infolist():
            name = os.path.basename(member.filename)
            if not name or member.is_dir():
                continue
            with zf.open(member) as src, open(os.path.join(target, name), "wb") as dst:
                dst.write(src.read())
    print(f"[{dataset}] extracted {len(os.listdir(target))} files ({len(payload) / 1e6:.1f} MB)")
    return target


# ------------------------------------------------------------------------------
# 2. Parse the T-SQL install script
# ------------------------------------------------------------------------------
@dataclass
class Column:
    name: str
    sql_type: str  # base type, lower-case, e.g. "nvarchar", "decimal"
    args: Optional[str] = None  # e.g. "19,4", "max", "50"
    computed: Optional[str] = None  # T-SQL expression for computed columns


@dataclass
class Table:
    schema: str
    name: str
    columns: List[Column] = field(default_factory=list)
    data_file: Optional[str] = None
    field_terminator: str = "\t"
    row_terminator: str = "\n"
    row_terminator_is_hex: bool = False
    codepage: str = "65001"
    first_row: int = 1

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.name}"


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    # Remove "-- ..." to end-of-line, but not inside string literals.
    out_lines = []
    for line in sql.splitlines():
        in_str = False
        cut = len(line)
        i = 0
        while i < len(line):
            ch = line[i]
            if ch == "'":
                in_str = not in_str
            elif not in_str and line.startswith("--", i):
                cut = i
                break
            i += 1
        out_lines.append(line[:cut])
    return "\n".join(out_lines)


def _split_top_level(body: str) -> List[str]:
    """Split a column list on commas that are not inside parentheses or quotes."""
    parts, depth, in_str, buf = [], 0, False, []
    for ch in body:
        if ch == "'":
            in_str = not in_str
        elif not in_str:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append("".join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    if "".join(buf).strip():
        parts.append("".join(buf).strip())
    return parts


def _matching_paren(text: str, open_idx: int) -> int:
    depth, in_str = 0, False
    for i in range(open_idx, len(text)):
        ch = text[i]
        if ch == "'":
            in_str = not in_str
        elif not in_str:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return i
    raise ValueError("Unbalanced parentheses in CREATE TABLE")


_TERMINATOR_ESCAPES = {"\\t": "\t", "\\n": "\n", "\\r": "\r", "\\0": "\0"}


def _decode_terminator(value: str, is_row: bool) -> str:
    if value.lower().startswith("0x"):
        return bytes.fromhex(value[2:]).decode("latin-1")
    for k, v in _TERMINATOR_ESCAPES.items():
        value = value.replace(k, v)
    return value


def parse_install_script(path: str) -> Dict[str, Table]:
    with open(path, "r", encoding="utf-8-sig") as fh:
        sql = _strip_comments(fh.read())

    # User-defined alias types: CREATE TYPE [Name] FROM nvarchar(50) NULL;
    udts = {}
    for m in re.finditer(r"CREATE\s+TYPE\s+\[?(\w+)\]?\s+FROM\s+(\w+)(?:\s*\(([^)]*)\))?", sql, re.I):
        udts[m.group(1).lower()] = (m.group(2).lower(), m.group(3))

    tables: Dict[str, Table] = {}
    for m in re.finditer(r"CREATE\s+TABLE\s+\[(\w+)\]\.\[(\w+)\]\s*\(", sql, re.I):
        open_idx = m.end() - 1
        body = sql[open_idx + 1 : _matching_paren(sql, open_idx)]
        table = Table(schema=m.group(1), name=m.group(2))
        for item in _split_top_level(body):
            col = re.match(r"\[(\w+)\]\s+(.*)$", item, re.S)
            if not col:
                continue  # table-level CONSTRAINT / PRIMARY KEY etc.
            name, rest = col.group(1), col.group(2).strip()
            comp = re.match(r"AS\s+(.*?)(\s+PERSISTED.*)?$", rest, re.S | re.I)
            if comp:
                table.columns.append(Column(name, "computed", computed=comp.group(1).strip()))
                continue
            t = re.match(r"\[?(\w+)\]?\s*(?:\(\s*([^)]*?)\s*\))?", rest)
            sql_type, args = t.group(1).lower(), t.group(2)
            if sql_type in udts:
                sql_type, args = udts[sql_type]
            table.columns.append(Column(name, sql_type, args))
        tables[table.full_name.lower()] = table

    for m in re.finditer(
        r"BULK\s+INSERT\s*\[(\w+)\]\.\[(\w+)\]\s+FROM\s+'[^']*?([^'\\/)]+)'\s+WITH\s*\((.*?)\)\s*;",
        sql,
        re.I | re.S,
    ):
        table = tables.get(f"{m.group(1)}.{m.group(2)}".lower())
        if table is None:
            continue
        table.data_file = m.group(3)
        opts = m.group(4)

        def opt(name, default=None):
            mm = re.search(name + r"\s*=\s*'([^']*)'", opts, re.I) or re.search(name + r"\s*=\s*(\d+)", opts, re.I)
            return mm.group(1) if mm else default

        table.field_terminator = _decode_terminator(opt("FIELDTERMINATOR", "\\t"), False)
        row_term = opt("ROWTERMINATOR", "\\n")
        table.row_terminator = _decode_terminator(row_term, True)
        table.row_terminator_is_hex = row_term.lower().startswith("0x")
        table.codepage = opt("CODEPAGE", "65001")
        table.first_row = int(opt("FIRSTROW", "1"))
    return tables


# ------------------------------------------------------------------------------
# 3. Read a data file exactly the way BULK INSERT would
# ------------------------------------------------------------------------------
def _row_terminator_regex(table: Table) -> "re.Pattern":
    # SQL Server quirk: a '\n' given as an escape in ROWTERMINATOR also matches
    # CR+LF, so '&|\n' matches both "&|\n" and "&|\r\n". Hex terminators
    # ('0x0a') are taken literally.
    parts = [re.escape(p) for p in table.row_terminator.split("\n")]
    sep = r"\n" if table.row_terminator_is_hex else r"\r?\n"
    return re.compile(sep.join(parts))


def _clean(value: str) -> Optional[str]:
    if value == "":
        return None  # empty field -> NULL (BULK INSERT without KEEPNULLS)
    if value == "\0":
        return ""  # bcp writes empty strings as a single NUL character
    return value


def read_data_file(path: str, table: Table) -> List[List[Optional[str]]]:
    with open(path, "rb") as fh:
        raw = fh.read()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        text = raw.decode("utf-16")
    elif table.codepage in ("65001", "RAW", "ACP", "OEM"):
        text = raw.decode("utf-8-sig")
    else:
        text = raw.decode(f"cp{table.codepage}")

    # Every column has a field in the file, computed ones included (BULK INSERT
    # ignores those values; we recompute them later).
    n_cols = len(table.columns)
    ft = table.field_terminator
    row_re = _row_terminator_regex(table)

    # Fast path: split into rows, then into fields.
    lines = row_re.split(text)
    if lines and lines[-1].strip("\r\n") == "":
        lines.pop()
    lines = lines[table.first_row - 1 :]
    split = [line.split(ft) for line in lines]
    if all(len(f) == n_cols for f in split):
        return [[_clean(v) for v in f] for f in split]

    # Slow path, identical to how BULK INSERT reads: the first n-1 fields end at
    # the field terminator, the last one at the row terminator. This lets
    # fields contain line breaks (e.g. ProductReview.Comments).
    rows, pos, end, row_no = [], 0, len(text), 0
    while pos < end:
        if text[pos:].strip("\r\n\x00 ") == "":
            break
        fields = []
        for _ in range(n_cols - 1):
            nxt = text.find(ft, pos)
            if nxt < 0:
                raise ValueError(f"{table.full_name}: unexpected end of file in row {row_no + 1}")
            fields.append(text[pos:nxt])
            pos = nxt + len(ft)
        m = row_re.search(text, pos)
        last_end = m.start() if m else end
        fields.append(text[pos:last_end])
        pos = m.end() if m else end
        row_no += 1
        if row_no >= table.first_row:
            rows.append([_clean(v) for v in fields])
    return rows


# ------------------------------------------------------------------------------
# 4. Spark: types, computed columns, write
# ------------------------------------------------------------------------------
def spark_cast_expr(col: Column) -> str:
    """Spark SQL expression that converts the raw string column into the right type."""
    c = f"`{col.name}`"
    t, a = col.sql_type, (col.args or "")
    if t in ("int",):
        return f"CAST({c} AS INT)"
    if t == "bigint":
        return f"CAST({c} AS BIGINT)"
    if t in ("smallint", "tinyint"):
        return f"CAST({c} AS SMALLINT)"
    if t == "bit":
        return f"CAST({c} AS BOOLEAN)"
    if t in ("decimal", "numeric"):
        p, _, s = a.partition(",")
        return f"CAST({c} AS DECIMAL({p.strip() or 18},{s.strip() or 0}))"
    if t == "money":
        return f"CAST({c} AS DECIMAL(19,4))"
    if t == "smallmoney":
        return f"CAST({c} AS DECIMAL(10,4))"
    if t == "float":
        return f"CAST({c} AS DOUBLE)"
    if t == "real":
        return f"CAST({c} AS FLOAT)"
    if t == "date":
        return f"CAST({c} AS DATE)"
    if t in ("datetime", "datetime2", "smalldatetime"):
        return f"CAST({c} AS TIMESTAMP)"
    if t in ("varbinary", "binary", "image"):
        return f"unhex(regexp_replace({c}, '^0x', ''))"
    return c  # character types, xml, uniqueidentifier, geography, hierarchyid, time, ...


# Computed columns: translate the T-SQL expressions used in AdventureWorks to Spark SQL.
COMPUTED_SPARK = {
    "sales.customer.accountnumber": "concat('AW', lpad(CAST(CustomerID AS STRING), 8, '0'))",
    "purchasing.purchaseorderdetail.linetotal": "CAST(coalesce(OrderQty * UnitPrice, 0) AS DECIMAL(19,4))",
    "purchasing.purchaseorderdetail.stockedqty": "CAST(coalesce(ReceivedQty - RejectedQty, 0) AS DECIMAL(9,2))",
    "purchasing.purchaseorderheader.totaldue": "CAST(coalesce(SubTotal + TaxAmt + Freight, 0) AS DECIMAL(19,4))",
    "sales.salesorderdetail.linetotal": "CAST(coalesce(UnitPrice * (1.0 - UnitPriceDiscount) * OrderQty, 0) AS DECIMAL(38,6))",
    "sales.salesorderheader.salesordernumber": "concat('SO', CAST(SalesOrderID AS STRING))",
    "sales.salesorderheader.totaldue": "CAST(coalesce(SubTotal + TaxAmt + Freight, 0) AS DECIMAL(19,4))",
    "production.workorder.stockedqty": "coalesce(OrderQty - ScrappedQty, 0)",
}


def computed_expr(table: Table, col: Column) -> str:
    key = f"{table.full_name}.{col.name}".lower()
    if key in COMPUTED_SPARK and COMPUTED_SPARK[key]:
        return COMPUTED_SPARK[key]
    # Otherwise keep the value shipped in the data file, e.g.
    # Employee.OrganizationLevel and Document.DocumentLevel (hierarchyid.GetLevel()).
    if "getlevel()" in (col.computed or "").lower():
        return f"CAST(`{col.name}` AS SMALLINT)"
    return f"CAST(`{col.name}` AS STRING)"


def target_table_name(dataset: str, table: Table) -> str:
    prefix = TABLE_PREFIX[dataset]
    if USE_LAKEHOUSE_SCHEMAS:
        name = f"{prefix}_{table.schema}.{table.name}"
    else:
        name = f"{prefix}_{table.schema}_{table.name}" if dataset == "oltp" else f"{prefix}_{table.name}"
    return name.lower() if LOWERCASE_NAMES else name


def load_table(spark, dataset: str, folder: str, table: Table) -> int:
    from pyspark.sql.types import StringType, StructField, StructType

    rows = read_data_file(os.path.join(folder, table.data_file), table)
    string_schema = StructType([StructField(c.name, StringType(), True) for c in table.columns])

    try:
        import pandas as pd

        pdf = pd.DataFrame(rows, columns=[c.name for c in table.columns], dtype=object)
        df = spark.createDataFrame(pdf, schema=string_schema)
    except ImportError:
        df = spark.createDataFrame(rows, schema=string_schema)

    # 1) typed base columns, 2) computed columns (which may reference typed columns)
    base = [f"{spark_cast_expr(c)} AS `{c.name}`" if c.sql_type != "computed" else f"`{c.name}`" for c in table.columns]
    df = df.selectExpr(*base)
    final = [
        f"{computed_expr(table, c)} AS `{c.name}`" if c.sql_type == "computed" else f"`{c.name}`" for c in table.columns
    ]
    df = df.selectExpr(*final)

    target = target_table_name(dataset, table)
    if USE_LAKEHOUSE_SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target.split('.')[0]}")
    (
        df.write.format(TABLE_FORMAT)
        .mode(WRITE_MODE)
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    return len(rows)


def main(spark=None):
    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
    # Faster pandas -> Spark conversion (falls back automatically if unavailable).
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    if RAW_DIR.startswith("/lakehouse/default") and not os.path.isdir("/lakehouse/default"):
        raise RuntimeError(
            "No default Lakehouse is attached to this notebook. Attach one in the "
            "Lakehouse explorer (left pane), or set RAW_DIR to another folder."
        )

    wanted = {t.lower() for t in ONLY_TABLES}
    summary = []
    for dataset in DATASETS:
        folder = download_and_extract(dataset, RAW_DIR)
        tables = parse_install_script(os.path.join(folder, SOURCES[dataset]["sql"]))
        to_load = [t for t in tables.values() if t.data_file and (not wanted or t.full_name.lower() in wanted)]
        print(f"[{dataset}] {len(to_load)} tables to load")
        for t in sorted(to_load, key=lambda x: x.full_name):
            start = time.time()
            try:
                n = load_table(spark, dataset, folder, t)
                summary.append((dataset, t.full_name, target_table_name(dataset, t), n, "ok"))
                print(f"  {t.full_name:<45} -> {target_table_name(dataset, t):<45} {n:>9,} rows  {time.time() - start:5.1f}s")
            except Exception as exc:  # keep going; report at the end
                summary.append((dataset, t.full_name, target_table_name(dataset, t), 0, f"FAILED: {exc}"))
                print(f"  {t.full_name:<45} FAILED: {exc}")

    failed = [s for s in summary if s[4] != "ok"]
    print(f"\nDone: {len(summary) - len(failed)} tables loaded, {len(failed)} failed, "
          f"{sum(s[3] for s in summary):,} rows in total.")
    for s in failed:
        print(f"  {s[1]}: {s[4]}")
    return summary


if __name__ == "__main__":
    summary = main(globals().get("spark"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
