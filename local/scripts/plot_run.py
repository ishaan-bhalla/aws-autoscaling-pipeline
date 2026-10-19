"""Plot results/local_run.csv: queue backlog against the number of workers."""
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
df = pd.read_csv(ROOT / "results" / "local_run.csv")
df["minutes"] = df.seconds / 60

fig, ax = plt.subplots(figsize=(10, 4.8), dpi=150)
ax.plot(df.minutes, df.visible, color="#ff9900", lw=2, label="Waiting jobs (visible messages)")
ax.plot(df.minutes, df.in_flight, color="#ffcc80", lw=1.5, ls="--", label="Jobs in progress")
ax.axhline(10, color="#ff9900", lw=0.8, ls=":")
ax.text(df.minutes.max(), 10.5, "scale-out threshold (10)", ha="right", va="bottom", fontsize=8, color="#b36b00")
ax.set_xlabel("Minutes since the 120 files landed")
ax.set_ylabel("Messages on the job queue")
ax2 = ax.twinx()
ax2.step(df.minutes, df.workers, where="post", color="#232f3e", lw=2.2, label="Workers (instances)")
ax2.set_ylabel("Workers")
ax2.set_ylim(0, 6)
for _, r in df[df.action.notna() & (df.action != "")].iterrows():
    ax2.annotate("", xy=(r.minutes, r.workers), xytext=(r.minutes, r.workers + (-0.6 if "out" in r.action else 0.6)),
                 arrowprops=dict(arrowstyle="->", color="#232f3e", lw=0.8))
lines = ax.get_legend_handles_labels()[0] + ax2.get_legend_handles_labels()[0]
ax.legend(lines, [l.get_label() for l in lines], loc="upper right", fontsize=8, frameon=False)
ax.set_title(f"Local re-run of the scaling policy: {int(df.done.max())} files, 1 to {df.workers.max()} workers and back",
             loc="left", fontsize=11)
for a in (ax, ax2):
    a.spines["top"].set_visible(False)
fig.tight_layout()
fig.savefig(ROOT / "docs" / "local_scaling_run.png")
print("docs/local_scaling_run.png")
