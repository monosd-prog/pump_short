from __future__ import annotations

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def plot_winrate_by_bucket(df: pd.DataFrame, bucket_col: str, ax: plt.Axes | None = None) -> plt.Axes:
    if ax is None:
        _, ax = plt.subplots(figsize=(8, 4))

    plot_df = df.copy()
    plot_df = plot_df[plot_df[bucket_col].notna()]
    sns.barplot(data=plot_df, x=bucket_col, y="winrate", ax=ax, color="#4C72B0")
    ax.set_title(f"Winrate by {bucket_col}")
    ax.set_xlabel(bucket_col)
    ax.set_ylabel("Winrate")
    ax.tick_params(axis="x", rotation=45)
    return ax
