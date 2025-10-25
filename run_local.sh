set -euo pipefail

spark-submit --master local[*] problem1.py -- \
  --input "/home/ubuntu/fall-2025-a06-shuyu-M/data/logs/*.log" \
  --output_dir "/home/ubuntu/fall-2025-a06-shuyu-M/data/output"
