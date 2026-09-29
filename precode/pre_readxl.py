"""Python port of precode-r/pre_readxl.R -- reads a named Excel Table (an
actual Excel "Table" object, not just a cell range) off a given sheet, then
applies a fixed per-column schema (date/num) on top.

polars' read_excel has native `table_name` support (polars >= 1.20), so this
needs no cell-range lookup or openpyxl at all -- a real simplification over
an earlier pandas+openpyxl version of this file, not just a like-for-like
swap (per the user's 2026-09-28 "no pandas anywhere" instruction).

Real deviation from R, unavoidable given the libraries: readxl's
`col_types='text'` forces every cell to text first, so schemaed_readtable_excel
always re-parses from a string. polars' calamine engine hands back
already-typed columns instead (Int64/Float64/Date where it can infer one,
String otherwise). This port works WITH that typing instead of fighting it: a
'date' column accepts an already-parsed Date/Datetime cell directly, a
dash-containing string (ISO-ish text date), or a bare Excel serial number
(same 1899-12-30 origin R uses) -- covering the same three real-world cases
R's type-detection branch does.
"""

import datetime
import re

import polars as pl


def buildcolnames(columns):
    return [re.sub(r'[^0-9a-zA-Z]+', '_', str(c)).strip('_').lower() for c in columns]


def readtable_excel(excelfile, sheet, table_name):
    loadtable = pl.read_excel(excelfile, sheet_name=sheet, table_name=table_name,
                               infer_schema_length=None)
    print('raw loading')
    print(loadtable.schema)
    loadtable.columns = buildcolnames(loadtable.columns)
    print('build colnames')
    print(loadtable.schema)
    return loadtable


EXCEL_DATE_ORIGIN = datetime.date(1899, 12, 30)


def _cast_date_cell(v):
    if v is None:
        return None
    if isinstance(v, datetime.datetime):
        return v.date()
    if isinstance(v, datetime.date):
        return v
    s = str(v)
    if '-' in s:
        return datetime.date.fromisoformat(s[:10])
    return EXCEL_DATE_ORIGIN + datetime.timedelta(days=float(s))


def schemaed_readtable_excel(excelfile, sheet, table_name, schema):
    loadtable = readtable_excel(excelfile, sheet, table_name)

    for targetcol, coltyping in schema.items():
        print(f'doing {targetcol}')
        if targetcol not in loadtable.columns:
            print(f'WARNING: {targetcol} column not in this table')
            continue
        if coltyping == 'date':
            loadtable = loadtable.with_columns(
                pl.col(targetcol).map_elements(_cast_date_cell, return_dtype=pl.Date)
            )
        elif coltyping == 'num':
            loadtable = loadtable.with_columns(
                pl.col(targetcol).cast(pl.Float64, strict=False)
            )

    # R's readxl col_types='text' forces every column to text up front, so
    # any column NOT re-typed by the schema above stays plain text in R too.
    # Force the same end state here, both for R parity and because a stray
    # non-string dtype on one of these columns can otherwise break a later
    # union with another table's same-named column.
    #
    # Also strip leading/trailing whitespace: confirmed via the raw XLSX XML
    # (xl/sharedStrings.xml has entries like `xml:space="preserve">Caifan
    # </t>`) that some cells genuinely have a stray trailing space stored in
    # the file itself - calamine (polars' engine) faithfully preserves it,
    # readxl (R) evidently trims it (readxl's trim_ws=TRUE default). Matching
    # R's apparent behavior here, confirmed against live R 2026-09-28 (closed
    # a real item_id mismatch category in 2.1.a, not just cosmetic - item_id
    # incorporates this text).
    #
    # A whitespace-ONLY cell (confirmed via raw read: a literal single space
    # ' ', not null) strips down to '' - readxl's trim_ws=TRUE goes one step
    # further and treats that as a true NA, not empty string. Match that too:
    # R's paste() then stringifies the NA as literal "NA" text downstream
    # (2.1.a's item_id), which an empty string would NOT reproduce.
    for c in loadtable.columns:
        if c in schema:
            continue
        stripped = pl.col(c).cast(pl.Utf8, strict=False).str.strip_chars()
        loadtable = loadtable.with_columns(
            pl.when(stripped == '').then(None).otherwise(stripped).alias(c)
        )

    print('type casted dataframe')
    print(loadtable.schema)
    return loadtable
