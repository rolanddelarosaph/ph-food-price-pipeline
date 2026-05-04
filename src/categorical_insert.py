import pandas as pd
import mysql.connector

# ── Connect to MySQL ──────────────────────────────────────────
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Hunteradrian2604",        # ← put your MySQL root password here if any
)
cursor = conn.cursor()

# ── Create database ───────────────────────────────────────────
cursor.execute("CREATE DATABASE IF NOT EXISTS categorical_database")
cursor.execute("USE categorical_database")

# ── File map ──────────────────────────────────────────────────
files = {
    "sales_a": "/Users/rolanddelarosa/Desktop/Categorical Data Analysis/SQL/Sales_A.xlsx",
    "sales_b": "/Users/rolanddelarosa/Desktop/Categorical Data Analysis/SQL/Sales_B.xlsx",
    "sales_c": "/Users/rolanddelarosa/Desktop/Categorical Data Analysis/SQL/SALES_C.xlsx",
    "sales_d": "/Users/rolanddelarosa/Desktop/Categorical Data Analysis/SQL/Sales_D.xlsx",
    "sales_e": "/Users/rolanddelarosa/Desktop/Categorical Data Analysis/SQL/Sales_E.xlsx",
}

# ── Load & insert each file ───────────────────────────────────
for table, path in files.items():
    df = pd.read_excel(path)
    df = df.where(pd.notnull(df), None)   # replace NaN with None (SQL NULL)

    # Auto-build CREATE TABLE from dataframe columns
    type_map = {"int64": "INT", "float64": "DECIMAL(15,2)", "object": "VARCHAR(50)", "datetime64[us]": "DATE", "string": "VARCHAR(50)"}
    col_defs = ", ".join(f"`{c}` {type_map.get(str(df[c].dtype), 'VARCHAR(100)')}" for c in df.columns)
    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
    cursor.execute(f"CREATE TABLE `{table}` ({col_defs})")

    # Insert rows
    placeholders = ", ".join(["%s"] * len(df.columns))
    cols = ", ".join(f"`{c}`" for c in df.columns)
    sql = f"INSERT INTO `{table}` ({cols}) VALUES ({placeholders})"
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    cursor.executemany(sql, rows)
    conn.commit()
    print(f"✓ {table}: {len(rows)} rows inserted")

cursor.close()
conn.close()
print("\nDone! Open DBeaver and refresh categorical_database.")
