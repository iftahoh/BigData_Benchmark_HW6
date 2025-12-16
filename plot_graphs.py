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

    # מיפוי שמות יפים למקרא (כמו בתמונה ששלחת)
    # המפתח הוא השם בקוד, הערך הוא השם שיופיע בגרף
    name_mapping = {
        "DuckDB": "DuckDB",
        "SQLite_No_Index": "SQLite",
        "SQLite_With_Index": "SQLite+Index"
    }

    # קבלת רשימת השאילתות שהורצו
    queries = df['Query'].unique()
    queries.sort()

    print(f"Generating graphs for {len(queries)} queries...")

    # לולאה על כל שאילתה ושאילתה
    for q_id in queries:
        plt.figure(figsize=(10, 6))

        # סינון הנתונים לשאילתה הנוכחית
        q_data = df[df['Query'] == q_id]

        # ציור קו לכל קונפיגורציה
        # הסדר חשוב כדי שהצבעים יהיו קבועים (DuckDB ראשון, וכו')
        configurations = ["DuckDB", "SQLite_No_Index", "SQLite_With_Index"]

        for config in configurations:
            # בדיקה אם הקונפיגורציה הזו קיימת בתוצאות של השאילתה הזו
            if config in q_data['Configuration'].unique():
                subset = q_data[q_data['Configuration'] == config].sort_values('SF')

                # השם היפה למקרא
                label_name = name_mapping.get(config, config)

                # הציור עצמו: סקאלה ליניארית עם נקודות
                plt.plot(subset['SF'], subset['Time'],
                         marker='o',  # עיגולים בנקודות
                         linewidth=2,  # עובי קו
                         markersize=6,  # גודל נקודה
                         label=label_name)  # השם במקרא

        # עיצוב הגרף כמו בתמונה
        plt.title(f'Query {q_id} Performance Comparison', fontsize=14, fontweight='bold')
        plt.xlabel('Scale Factor (SF)', fontsize=12)
        plt.ylabel('Execution Time (seconds)', fontsize=12)

        # המקרא (בצד שמאל למעלה)
        plt.legend(loc='upper left', frameon=True, fontsize=10)

        # רשת עדינה ברקע
        plt.grid(True, linestyle='-', alpha=0.3)

        # גבולות צירים - מתחילים מ-0 כדי להיראות כמו בתמונה
        plt.xlim(left=0)
        plt.ylim(bottom=0)

        # שמירה
        plt.tight_layout()
        plt.savefig(f"graphs/query_{q_id}.png", dpi=100)
        plt.close()

    # --- יצירת גרף מסכם (ממוצע) באותו עיצוב ---
    print("Generating summary graph...")
    plt.figure(figsize=(10, 6))

    summary = df.groupby(['Configuration', 'SF'])['Time'].mean().reset_index()

    for config in configurations:
        if config in summary['Configuration'].unique():
            subset = summary[summary['Configuration'] == config].sort_values('SF')
            label_name = name_mapping.get(config, config)

            plt.plot(subset['SF'], subset['Time'],
                     marker='o', linewidth=2, label=label_name)

    plt.title('Average Query Performance (Summary)', fontsize=14, fontweight='bold')
    plt.xlabel('Scale Factor (SF)', fontsize=12)
    plt.ylabel('Average Time (seconds)', fontsize=12)
    plt.legend(loc='upper left')
    plt.grid(True, linestyle='-', alpha=0.3)
    plt.xlim(left=0)
    plt.ylim(bottom=0)

    plt.savefig("graphs/summary_average.png", dpi=100)
    plt.close()

    print("Done! Graphs saved in 'graphs' folder.")


if __name__ == "__main__":
    generate_graphs()