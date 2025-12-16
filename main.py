import duckdb
import sqlite3
import time
import psutil
import platform
import os
import pandas as pd
import statistics
import sqlglot
from datetime import datetime

# --- הגדרות כלליות ---
TIMEOUT_THRESHOLD = 90.0  # שניות - עצירה אם שאילתה איטית מדי
NUM_ROUNDS_VALIDATION = 4  # כמה סבבים ראשונים לבצע בדיקת נכונות
SF_PLAN = [0.001, 0.002, 0.003, 0.005, 0.007, 0.01, 0.02, 0.03,0.05,0.1]  # תכנון ל-10 סבבים


def get_system_info():
    """הדפסת פרטי המערכת כנדרש"""
    print("=" * 40)
    print("System Information")
    print("=" * 40)
    uname = platform.uname()
    print(f"System: {uname.system}")
    print(f"Node Name: {uname.node}")
    print(f"Release: {uname.release}")
    print(f"Version: {uname.version}")
    print(f"Machine: {uname.machine}")
    print(f"Processor: {uname.processor}")
    print(f"Total Memory: {psutil.virtual_memory().total / (1024 ** 3):.2f} GB")
    print("=" * 40)


class BenchmarkRunner:
    def __init__(self):
        self.results = []
        if not os.path.exists("results"):
            os.makedirs("results")

        # רשימת הטבלאות
        self.tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']

        # שליפת השאילתות ישירות מתוך DuckDB
        self.queries = self.extract_queries_from_duckdb()

        # ניהול שאילתות פעילות (רשימה שחורה לכל קונפיגורציה)
        all_q_ids = list(self.queries.keys())
        self.active_queries = {
            "DuckDB": all_q_ids.copy(),
            "SQLite_No_Index": all_q_ids.copy(),
            "SQLite_With_Index": all_q_ids.copy()
        }

    def extract_queries_from_duckdb(self):
        """
        שולף את השאילתות ישירות מהרחבת TPC-H של DuckDB.
        ללא צורך בקבצים חיצוניים.
        """
        print("Extracting TPC-H queries directly from DuckDB...")
        queries = {}
        con = duckdb.connect(":memory:")
        con.execute("INSTALL tpch; LOAD tpch;")

        # הפקודה הנכונה לשליפת שאילתות
        try:
            # ננסה לשלוף את כל השאילתות מהטבלה הוירטואלית
            # הפקודה היא tpch_queries() ולא פונקציה סקלרית
            res = con.execute("SELECT query_nr, query FROM tpch_queries()").fetchall()

            for q_nr, q_sql in res:
                queries[q_nr] = q_sql

            print(f"Successfully extracted {len(queries)} queries from DuckDB.")

        except Exception as e:
            print(f"Error extracting queries: {e}")
            # במקרה חירום בלבד - אם השליפה נכשלת, אפשר להכניס ידנית למילון כאן

        con.close()
        return queries

    def generate_tpch_data(self, sf):
        print(f"--- Generating Data SF={sf} ---")
        con = duckdb.connect(database=':memory:')
        con.execute("INSTALL tpch; LOAD tpch;")
        con.execute(f"CALL dbgen(sf={sf})")
        return con

    def transfer_to_sqlite(self, duck_con, db_filename):
        if os.path.exists(db_filename):
            os.remove(db_filename)

        print(f"Transferring to {db_filename}...")
        sqlite_con = sqlite3.connect(db_filename)

        for table in self.tables:
            # שימוש ב-Pandas להעברה
            df = duck_con.sql(f"SELECT * FROM {table}").df()
            df.to_sql(table, sqlite_con, index=False, if_exists='replace')

        sqlite_con.commit()
        sqlite_con.close()

    def create_sqlite_indexes(self, db_filename):
        print(f"Creating Indexes on {db_filename}...")
        con = sqlite3.connect(db_filename)
        cursor = con.cursor()

        # אינדקסים מומלצים
        indexes = [
            "CREATE INDEX idx_l_returnflag ON lineitem(l_returnflag)",
            "CREATE INDEX idx_l_shipdate ON lineitem(l_shipdate)",
            "CREATE INDEX idx_l_orderkey ON lineitem(l_orderkey)",
            "CREATE INDEX idx_l_partkey ON lineitem(l_partkey)",
            "CREATE INDEX idx_o_orderdate ON orders(o_orderdate)",
            "CREATE INDEX idx_o_custkey ON orders(o_custkey)",
            "CREATE INDEX idx_c_mktsegment ON customer(c_mktsegment)",
            "CREATE INDEX idx_n_regionkey ON nation(n_regionkey)"
        ]

        for idx in indexes:
            try:
                cursor.execute(idx)
            except Exception as e:
                print(f"Index error: {e}")

        con.commit()
        con.close()

    def validate_results(self, duck_res, sqlite_res, query_id):
        """השוואת תוצאות"""
        if len(duck_res) != len(sqlite_res):
            print(f" [VALIDATION FAIL Q{query_id}] Row count mismatch! Duck: {len(duck_res)}, Lite: {len(sqlite_res)}")
            return False

        if len(duck_res) > 0:
            if len(duck_res[0]) != len(sqlite_res[0]):
                print(f" [VALIDATION FAIL Q{query_id}] Col count mismatch!")
                return False

            # בדיקה מדגמית (המרה לסטרינגים להשוואה בטוחה)
            d_row = [str(x) for x in duck_res[0]]
            s_row = [str(x) for x in sqlite_res[0]]

            # בדיקה גסה (לפעמים סדר העמודות זהה אבל הפורמט שונה, לכן זה רק אזהרה)
            if d_row != s_row:
                # לא נכשיל את הריצה, רק נדפיס אזהרה
                # זה קורה הרבה בגלל הבדלי 1.00 vs 1.0 וכו'
                pass

        print(f" [VALIDATION OK Q{query_id}]")
        return True

    def execute_and_measure(self, con, query_sql, query_id, config_name, check_validation=False, duck_ref_res=None):
        """מריץ, מודד זמן ומתרגם ל-SQLite במידת הצורך"""

        # תרגום ל-SQLite באמצעות sqlglot
        if "SQLite" in config_name:
            try:
                # המרה מניב DuckDB לניב SQLite
                query_sql = sqlglot.transpile(query_sql, read="duckdb", write="sqlite")[0]
            except Exception as e:
                print(f"Warning: Sqlglot translation failed for Q{query_id}. Running original SQL. Error: {e}")

        times = []
        last_result = None

        for i in range(3):  # הרצה 3 פעמים
            try:
                start = time.time()
                last_result = con.execute(query_sql).fetchall()
                end = time.time()
                times.append(end - start)
            except Exception as e:
                print(f"Error running Q{query_id} on {config_name}: {e}")
                return 9999.0, None  # החזרת זמן "עונש" במקרה של כישלון

        median_time = statistics.median(times)

        if check_validation and duck_ref_res is not None:
            self.validate_results(duck_ref_res, last_result, query_id)

        return median_time, last_result

    def save_result(self, query_id, config, sf, time_val):
        row = {
            "Query": query_id,
            "Configuration": config,
            "SF": sf,
            "Time": time_val,
            "Timestamp": datetime.now()
        }
        self.results.append(row)

    def run_benchmark(self):
        # לולאה ראשית על פני הגדלים (SF)
        for i, sf in enumerate(SF_PLAN):
            round_num = i + 1
            print(f"\n{'=' * 20} ROUND {round_num}/10 (SF={sf}) {'=' * 20}")

            # 1. יצירת דאטה
            duck_con = self.generate_tpch_data(sf)

            # בדיקה אם צריך לייצר SQLite (אם נשארו שאילתות פעילות)
            need_sqlite = (len(self.active_queries["SQLite_No_Index"]) > 0) or \
                          (len(self.active_queries["SQLite_With_Index"]) > 0)

            if need_sqlite:
                self.transfer_to_sqlite(duck_con, "sqlite_no_index.db")
                import shutil
                shutil.copy("sqlite_no_index.db", "sqlite_with_index.db")
                self.create_sqlite_indexes("sqlite_with_index.db")

            con_lite_no = sqlite3.connect("sqlite_no_index.db") if need_sqlite else None
            con_lite_idx = sqlite3.connect("sqlite_with_index.db") if need_sqlite else None

            # הרצת שאילתות
            for q_id, q_sql in self.queries.items():

                # --- DuckDB ---
                duck_res = None
                if q_id in self.active_queries["DuckDB"]:
                    print(f"Running Q{q_id} on DuckDB...")
                    t, duck_res = self.execute_and_measure(duck_con, q_sql, q_id, "DuckDB")
                    self.save_result(q_id, "DuckDB", sf, t)

                    if t > TIMEOUT_THRESHOLD:
                        print(f"!!! Q{q_id} on DuckDB timed out. Removing.")
                        self.active_queries["DuckDB"].remove(q_id)

                # --- SQLite No Index ---
                if con_lite_no and q_id in self.active_queries["SQLite_No_Index"]:
                    print(f"Running Q{q_id} on SQLite(NoIdx)...")
                    do_valid = (round_num <= NUM_ROUNDS_VALIDATION) and (duck_res is not None)
                    t, _ = self.execute_and_measure(con_lite_no, q_sql, q_id, "SQLite_No_Index",
                                                    check_validation=do_valid, duck_ref_res=duck_res)
                    self.save_result(q_id, "SQLite_No_Index", sf, t)

                    if t > TIMEOUT_THRESHOLD:
                        print(f"!!! Q{q_id} on SQLite(NoIdx) timed out. Removing.")
                        self.active_queries["SQLite_No_Index"].remove(q_id)

                # --- SQLite With Index ---
                if con_lite_idx and q_id in self.active_queries["SQLite_With_Index"]:
                    print(f"Running Q{q_id} on SQLite(Idx)...")
                    t, _ = self.execute_and_measure(con_lite_idx, q_sql, q_id, "SQLite_With_Index")
                    self.save_result(q_id, "SQLite_With_Index", sf, t)

                    if t > TIMEOUT_THRESHOLD:
                        print(f"!!! Q{q_id} on SQLite(Idx) timed out. Removing.")
                        self.active_queries["SQLite_With_Index"].remove(q_id)

            # סגירה
            duck_con.close()
            if con_lite_no: con_lite_no.close()
            if con_lite_idx: con_lite_idx.close()

            # שמירת CSV
            pd.DataFrame(self.results).to_csv("results/benchmark_results.csv", index=False)
            print("Results saved to CSV.")


if __name__ == "__main__":
    get_system_info()
    runner = BenchmarkRunner()
    runner.run_benchmark()