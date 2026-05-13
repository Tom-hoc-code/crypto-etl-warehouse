import pandas as pd
from pathlib import Path

staging_dir = Path(r"F:\project-coin\data\staging\coins")

# Lấy tất cả file .parquet trong các folder con
parquet_files = list(staging_dir.rglob("*.parquet"))

print(f"Tìm thấy {len(parquet_files)} file Parquet")

if parquet_files:
    # đọc 1 file đầu tiên để kiểm tra
    df = pd.read_parquet(parquet_files[0])
    print(df.head())