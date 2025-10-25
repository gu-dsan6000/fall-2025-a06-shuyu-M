
import os
import csv
import argparse
from pyspark.sql import SparkSession, functions as F

LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
PATTERN = r"\b(INFO|WARN|ERROR|DEBUG)\b"

def fmt_int(n: int) -> str:
    return f"{n:,}"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help='Path/glob to log files (e.g. "data/logs/*.log")')
    parser.add_argument("--output_dir", default="data/output", help="Directory to write the 3 outputs")
    parser.add_argument("--sample_n", type=int, default=10, help="Number of random sample rows")
    args = parser.parse_args()

    out_counts = os.path.join(args.output_dir, "problem1_counts.csv")
    out_sample = os.path.join(args.output_dir, "problem1_sample.csv")
    out_summary = os.path.join(args.output_dir, "problem1_summary.txt")
    os.makedirs(args.output_dir, exist_ok=True)

    spark = (
        SparkSession.builder.appName("Problem1-LogLevelDistribution")
        .getOrCreate()
    )

    # Read raw lines
    lines = spark.read.text(args.input).toDF("line")

    # Extract log level token; empty string -> null
    logs = (
        lines.withColumn("log_level", F.regexp_extract("line", PATTERN, 1))
             .withColumn("log_level", F.when(F.col("log_level") == "", F.lit(None)).otherwise(F.col("log_level")))
    )

    # Totals
    total_lines = lines.count()
    with_levels = logs.where(F.col("log_level").isNotNull()).cache()
    total_with_levels = with_levels.count()

    # Counts per level (restricted to expected levels, ordered by count desc then name)
    counts_df = (
        with_levels.groupBy("log_level").count()
        .filter(F.col("log_level").isin(LEVELS))
        .orderBy(F.desc("count"), F.asc("log_level"))
    )
    unique_levels_found = counts_df.select("log_level").distinct().count()

    # Collect counts to driver and write a single CSV file
    counts = counts_df.collect()
    with open(out_counts, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["log_level", "count"])
        for row in counts:
            w.writerow([row["log_level"], row["count"]])

    # Random sample of log entries with their levels
    sample_df = with_levels.orderBy(F.rand()).limit(args.sample_n) \
        .select(F.col("line").alias("log_entry"), F.col("log_level"))
    sample_rows = sample_df.collect()
    with open(out_sample, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)  # quotes fields with commas automatically
        w.writerow(["log_entry", "log_level"])
        for r in sample_rows:
            w.writerow([r["log_entry"], r["log_level"]])

    # Build summary text
    by_level_lines = []
    counts_map = {r["log_level"]: r["count"] for r in counts}
    for lvl in LEVELS:
        if lvl in counts_map:
            pct = (counts_map[lvl] / total_with_levels * 100.0) if total_with_levels else 0.0
            by_level_lines.append(f"  {lvl:<6}: {fmt_int(counts_map[lvl]).rjust(8)} ({pct:5.2f}%)")

    summary = []
    summary.append(f"Total log lines processed: {fmt_int(total_lines)}")
    summary.append(f"Total lines with log levels: {fmt_int(total_with_levels)}")
    summary.append(f"Unique log levels found: {unique_levels_found}")
    summary.append("")
    summary.append("Log level distribution:")
    summary.extend(by_level_lines)
    summary_text = "\n".join(summary) + "\n"

    with open(out_summary, "w", encoding="utf-8") as f:
        f.write(summary_text)

    spark.stop()

if __name__ == "__main__":
    main()
