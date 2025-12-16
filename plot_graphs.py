import pandas as pd
import matplotlib.pyplot as plt
import os


def generate_graphs():
    # בדיקה שהקובץ קיים
    csv_path = "results/benchmark_results.csv"
    if not os.path.exists(csv_path):
        print("Error: results/benchmark_results.csv not found. Run main.py first!")
        return

    # טעינת הנתונים
    df = pd.read_csv(csv_path)

    # יצירת תיקייה לגרפים
    if not os.path.exists("graphs"):
        os.makedirs("graphs")

    # קבלת רשימת השאילתות שהורצו
    queries = df['Query'].unique()
    queries.sort()

    print("Generating graphs for each query...")

    # --- חלק 1: גרף לכל שאילתה ---
    for q_id in queries:
        plt.figure(figsize=(10, 6))

        # סינון לפי שאילתה
        q_data = df[df['Query'] == q_id]

        # ציור קו לכל קונפיגורציה
        for config in q_data['Configuration'].unique():
            subset = q_data[q_data['Configuration'] == config].sort_values('SF')
            plt.plot(subset['SF'], subset['Time'], marker='o', label=config)

        plt.title(f'TPC-H Query {q_id} Performance')
        plt.xlabel('Scale Factor (SF)')
        plt.ylabel('Time (Seconds) - Log Scale')
        plt.legend()
        plt.grid(True)

        # שימוש בסקאלה לוגריתמית כי DuckDB מהיר בהרבה מ-SQLite
        plt.yscale('log')

        # שמירה
        plt.savefig(f"graphs/query_{q_id}.png")
        plt.close()

    # --- חלק 2: גרף מסכם (ממוצע) ---
    print("Generating summary graph...")
    plt.figure(figsize=(12, 8))

    # חישוב ממוצע זמן ריצה לכל SF וקונפיגורציה (על פני כל השאילתות)
    summary = df.groupby(['Configuration', 'SF'])['Time'].mean().reset_index()

    for config in summary['Configuration'].unique():
        subset = summary[summary['Configuration'] == config].sort_values('SF')
        plt.plot(subset['SF'], subset['Time'], marker='s', linestyle='--', linewidth=2, label=config)

    plt.title('Average Query Execution Time (Summary)')
    plt.xlabel('Scale Factor (SF)')
    plt.ylabel('Average Time (Seconds) - Log Scale')
    plt.legend()
    plt.grid(True, which="both", ls="-")
    plt.yscale('log')

    plt.savefig("graphs/summary_average.png")
    plt.close()

    print("Done! Graphs saved in 'graphs' folder.")


if __name__ == "__main__":
    generate_graphs()