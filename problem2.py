
import argparse
import os
from pathlib import Path
import glob

# Spark & DataFrame bits
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Plotting
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# CLI
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cluster Usage Analysis")
    p.add_argument(
        "--input",
        default="data/raw/application_*/container_*.log",
        help='Glob of input container logs '
             '(default: "data/raw/application_*/container_*.log")'
    )
    p.add_argument(
        "--output_dir",
        default="data/output",
        help='Output directory (default: "data/output")'
    )
    p.add_argument(
        "--skip-spark",
        action="store_true",
        help="Skip Spark phase; re-use CSVs to regenerate figures only."
    )
    return p.parse_args()


def ensure_outdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def expand_input(pattern: str) -> list[str]:
    """Expand a glob pattern with Python first so we can fail early with
    good diagnostics (and not rely on Spark to expand)."""
    paths = sorted(glob.glob(pattern))
    print(f"[problem2] Using input: {pattern}")
    print(f"[problem2] Output dir: {OUT_DIR}")
    print(f"[problem2] Spark phase input: {pattern}")
    print(f"[problem2] Matched files: {len(paths)}")
    if paths:
        print(f"[problem2] Example file: {paths[0]}")
    return paths

# Spark phase
def run_spark(paths: list[str], out_dir: Path) -> None:
    """Read logs, extract timestamps/application/cluster, write CSVs."""
    # Create Spark (force local[*]) and keep ANSI OFF -> try_to_timestamp won't error
    spark = (
        SparkSession.builder
        .appName("Problem2–Cluster-Usage-Analysis")
        .config("spark.sql.ansi.enabled", "false")  # 你加的 ANSI 关闭保留
        .getOrCreate()
    )
    spark.conf.set("spark.sql.ansi.enabled", "false")

    # Load logs as a single "line" column; include filename via input_file_name()
    df = (
        spark.read.text(paths)
        .withColumnRenamed("value", "line")
        .withColumn("filename", F.input_file_name())
    )

    # --- 1) Extract application_id first (e.g., "application_1485248649253_0001")
    # We will also use it to derive cluster_id (the long numeric part before the underscore).
    # Regex picks the first application_##########_#### occurrence in each line.
    df = df.withColumn(
        "application_id",
        F.regexp_extract(F.col("line"), r"(application_\d+_\d+)", 1)
    )

    # If application_id was not present in the line, try to infer cluster_id from the path,
    # then optionally fill in application_id later when seen. For timeline purposes, we only
    # aggregate rows that have a recognized application_id.
    # Derive cluster_id from filename path (folder "application_<cluster>_..."):
    df = df.withColumn(
        "cluster_from_path",
        F.regexp_extract(F.col("filename"), r"application_(\d+)", 1)
    )

    # Prefer cluster from application_id if present; otherwise fall back to path-derived:
    df = df.withColumn(
        "cluster_id_from_app",
        F.regexp_extract(F.col("application_id"), r"application_(\d+)_\d+", 1)
    ).withColumn(
        "cluster_id",
        F.when(F.col("cluster_id_from_app") != "", F.col("cluster_id_from_app"))
         .otherwise(F.col("cluster_from_path"))
    ).drop("cluster_id_from_app", "cluster_from_path")

    # --- 2) Parse timestamps:
    # The logs contain a mix of timestamp formats (examples seen in the dataset):
    #   "yy/MM/dd HH:mm:ss"
    #   "yyyy-MM-dd HH:mm:ss"
    #   "yyyy/MM/dd HH:mm:ss"
    #
    # We first extract the timestamp substring with a permissive regex, then try
    # multiple patterns with try_to_timestamp and coalesce them.
    ts_raw = F.regexp_extract(
        F.col("line"),
        r"(\d{2,4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2})",
        1
    )

    ts1 = F.to_timestamp(ts_raw, "yy/MM/dd HH:mm:ss")
    ts2 = F.to_timestamp(ts_raw, "yyyy-MM-dd HH:mm:ss")
    ts3 = F.to_timestamp(ts_raw, "yyyy/MM/dd HH:mm:ss")

    # Use ANSI-safe parser (Spark 3.4+: try_to_timestamp). Fallback via coalesce.
    ts_try1 = F.expr("try_to_timestamp(regexp_extract(line, '(\\d{2}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2})', 1), 'yy/MM/dd HH:mm:ss')")
    ts_try2 = F.expr("try_to_timestamp(regexp_extract(line, '(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})', 1), 'yyyy-MM-dd HH:mm:ss')")
    ts_try3 = F.expr("try_to_timestamp(regexp_extract(line, '(\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2})', 1), 'yyyy/MM/dd HH:mm:ss')")

    df = df.withColumn("ts_raw", ts_raw).withColumn(
        "ts",
        F.coalesce(ts_try1, ts_try2, ts_try3, ts1, ts2, ts3)  # tolerate format noise
    )

    # Keep only lines that actually have both application_id and a parsed timestamp
    df = df.where((F.col("application_id") != "") & (F.col("ts").isNotNull()))

    # 3) Build timeline per (cluster_id, application_id)
    # app_number is the numeric suffix of application_id (e.g. application_..._0137 -> 137)
    df = df.withColumn(
        "app_number",
        F.regexp_extract(F.col("application_id"), r"application_\d+_(\d+)", 1)
    )

    timeline = (
        df.groupBy("cluster_id", "application_id", "app_number")
          .agg(
              F.min("ts").alias("start_time"),
              F.max("ts").alias("end_time")
          )
          .orderBy(F.col("cluster_id").asc(), F.col("app_number").cast(T.IntegerType()).asc())
    )

    # Persist timeline
    out_timeline = out_dir / "problem2_timeline.csv"
    (timeline.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(out_timeline) + ".tmp"))
    # Move the single CSV out of the Spark folder
    _move_spark_single_csv(out_timeline)

    # 4) From timeline, compute per-application durations (minutes)
    # We’ll re-load the just-written CSV in Pandas for convenience of downstream stats/plots.
    tlf = pd.read_csv(out_timeline)
    tlf["start_time"] = pd.to_datetime(tlf["start_time"])
    tlf["end_time"]   = pd.to_datetime(tlf["end_time"])
    tlf["duration_min"] = (tlf["end_time"] - tlf["start_time"]).dt.total_seconds() / 60.0
    tlf = tlf[(tlf["duration_min"].notna()) & (tlf["duration_min"] > 0)]

    # 5) Cluster summary (num apps + temporal span + duration stats)
    g = tlf.groupby("cluster_id", as_index=False)
    cluster_summary = g.agg(
        num_apps=("application_id", "nunique"),
        min_start=("start_time", "min"),
        max_end=("end_time", "max"),
        median_duration_s=("duration_min", lambda s: float(pd.Series(s).median()) * 60.0),
        mean_duration_s=("duration_min", lambda s: float(pd.Series(s).mean()) * 60.0),
        p95_duration_s=("duration_min", lambda s: float(pd.Series(s).quantile(0.95)) * 60.0),
    )
    cluster_summary = cluster_summary.sort_values("cluster_id")

    out_summary = out_dir / "problem2_cluster_summary.csv"
    cluster_summary.to_csv(out_summary, index=False)

    # 6) Pretty stats.txt in expected style
    _write_pretty_stats(tlf, cluster_summary, out_dir)

    # 7) Bar chart (num apps per cluster)
    _plot_bar(cluster_summary, out_dir)

    # 8) Density plot (largest cluster ONLY, log x-axis, hist + KDE)
    _plot_largest_cluster_density(tlf, cluster_summary, out_dir)

    spark.stop()
    print("[problem2] Done.")


# CSV-only phase 
def regenerate_from_csv(out_dir: Path) -> None:
    """Re-render figures and stats from existing CSVs."""
    timeline_csv = out_dir / "problem2_timeline.csv"
    summary_csv  = out_dir / "problem2_cluster_summary.csv"

    assert timeline_csv.exists(), f"Missing {timeline_csv}"
    assert summary_csv.exists(),  f"Missing {summary_csv}"

    tlf = pd.read_csv(timeline_csv)
    tlf["start_time"] = pd.to_datetime(tlf["start_time"])
    tlf["end_time"]   = pd.to_datetime(tlf["end_time"])
    tlf["duration_min"] = (tlf["end_time"] - tlf["start_time"]).dt.total_seconds() / 60.0
    tlf = tlf[(tlf["duration_min"].notna()) & (tlf["duration_min"] > 0)]

    cluster_summary = pd.read_csv(summary_csv)

    _write_pretty_stats(tlf, cluster_summary, out_dir)
    _plot_bar(cluster_summary, out_dir)
    _plot_largest_cluster_density(tlf, cluster_summary, out_dir)
    print("[problem2] Re-generated figures from CSVs.")


# Stats & plotting helpers
def _write_pretty_stats(tlf: pd.DataFrame, cs: pd.DataFrame, out_dir: Path) -> None:
    """Write stats.txt in expected human-friendly style."""
    num_clusters = int(cs.shape[0])
    total_apps   = int(tlf["application_id"].nunique())
    avg_per      = (total_apps / num_clusters) if num_clusters else 0.0

    # Choose the count column name (compat)
    count_col = "num_applications" if "num_applications" in cs.columns else (
        "num_apps" if "num_apps" in cs.columns else None
    )
    if count_col is None:
        # Fallback in case of schema change
        count_col = "num_apps"
        if count_col not in cs.columns:
            cs = cs.rename(columns={cs.columns[1]: count_col})

    top = cs.sort_values(count_col, ascending=False).head(5)

    lines = [
        f"Total unique clusters: {num_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_per:.2f}",
        "",
        "Most heavily used clusters:",
    ]
    for _, r in top.iterrows():
        lines.append(f"  Cluster {r['cluster_id']}: {int(r[count_col])} applications")

    path = out_dir / "problem2_stats.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[problem2] Wrote {path}")


def _plot_bar(cs: pd.DataFrame, out_dir: Path) -> None:
    """Bar chart of number of applications per cluster, with value labels."""
    count_col = "num_applications" if "num_applications" in cs.columns else (
        "num_apps" if "num_apps" in cs.columns else cs.columns[1]
    )

    plt.figure(figsize=(12, 6))
    ax = sns.barplot(data=cs, x="cluster_id", y=count_col)
    ax.set_xlabel("cluster_id")
    ax.set_ylabel(count_col)

    # Add value labels on top of bars
    for p in ax.patches:
        height = p.get_height()
        ax.annotate(f"{int(height)}",
                    (p.get_x() + p.get_width() / 2, height),
                    ha="center", va="bottom", fontsize=9, xytext=(0, 3),
                    textcoords="offset points")

    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    out_path = out_dir / "problem2_bar_chart.png"
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"[problem2] Wrote {out_path}")


def _plot_largest_cluster_density(tlf: pd.DataFrame, cs: pd.DataFrame, out_dir: Path) -> None:
    """KDE + histogram of job durations for the largest cluster only (log x-axis)."""
    count_col = "num_applications" if "num_applications" in cs.columns else (
        "num_apps" if "num_apps" in cs.columns else cs.columns[1]
    )
    largest = cs.sort_values(count_col, ascending=False).iloc[0]["cluster_id"]

    sub = tlf[tlf["cluster_id"].astype(str) == str(largest)].copy()
    sub = sub[(sub["duration_min"].notna()) & (sub["duration_min"] > 0)]

    plt.figure(figsize=(12, 6))
    ax = sns.histplot(sub["duration_min"], kde=True)
    ax.set_xscale("log")
    ax.set_xlabel("duration_min (log scale)")
    ax.set_ylabel("Density")
    ax.set_title(f"Job duration distribution — cluster {largest} (n={len(sub)})")
    plt.tight_layout()

    out_path = out_dir / "problem2_density_plot.png"
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"[problem2] Wrote {out_path}")


def _move_spark_single_csv(target_csv: Path) -> None:
    """Spark writes part files under a directory; move/rename the single part
    to the requested CSV path and clean up the temp folder."""
    tmp_dir = Path(str(target_csv) + ".tmp")
    assert tmp_dir.exists(), f"Expected temp folder not found: {tmp_dir}"
    # Find the single part file
    part_files = list(tmp_dir.glob("part-*.csv"))
    if not part_files:
        # Some Spark distros write .csv + .crc or .csv with different prefix
        part_files = list(tmp_dir.glob("*.csv"))
    assert part_files, f"No CSV part files found in {tmp_dir}"
    part_files[0].replace(target_csv)
    # Remove the folder
    for p in tmp_dir.glob("*"):
        try:
            p.unlink()
        except Exception:
            pass
    try:
        tmp_dir.rmdir()
    except Exception:
        pass


# Entrypoint
if __name__ == "__main__":
    args = parse_args()
    OUT_DIR = Path(args.output_dir)
    ensure_outdir(OUT_DIR)

    if args.skip_spark:
        regenerate_from_csv(OUT_DIR)
    else:
        files = expand_input(args.input)
        if not files:
            raise SystemExit(f"[problem2] No files matched: {args.input}")
        run_spark(files, OUT_DIR)
