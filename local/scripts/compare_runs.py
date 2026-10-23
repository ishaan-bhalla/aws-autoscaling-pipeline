"""Side-by-side chart of several runs on shared axes.

    python compare_runs.py OUT.png RUN1.csv "title 1" RUN2.csv "title 2" ...
"""
import sys

import matplotlib.pyplot as plt
import pandas as pd

png, rest = sys.argv[1], sys.argv[2:]
runs = [(pd.read_csv(rest[i]), rest[i + 1]) for i in range(0, len(rest), 2)]
xmax = max(df.seconds.max() for df, _ in runs) / 60 + 0.5

fig, axes = plt.subplots(1, len(runs), figsize=(5.2 * len(runs), 4.6), dpi=150, sharey=True)
for n, (ax, (df, title)) in enumerate(zip(axes, runs)):
    m = df.seconds / 60
    ax.plot(m, df.visible, color="#ff9900", lw=2, label="Waiting (visible)")
    ax.plot(m, df.in_flight, color="#b36b00", lw=1.5, ls="--", label="In progress (in flight)")
    ax.set_xlim(0, xmax)
    ax.set_ylim(0, 125)
    ax.set_xlabel("Minutes since the files landed")
    ax.set_title(title, loc="left", fontsize=9.5)
    ax2 = ax.twinx()
    ax2.step(m, df.workers, where="post", color="#232f3e", lw=2.2, label="Workers")
    ax2.set_ylim(0, 6)
    if n == len(runs) - 1:
        ax2.set_ylabel("Workers")
    else:
        ax2.set_yticklabels([])
    done = df[df.done >= df.done.max()].seconds.min() / 60
    ax.axvline(done, color="#2e7d32", lw=1, ls=":")
    ax.text(done + 0.2, 62, f"last result\nwritten ({done:.1f} min)", color="#2e7d32", fontsize=8, va="center")
    for a in (ax, ax2):
        a.spines["top"].set_visible(False)
    if n == 0:
        lines = ax.get_legend_handles_labels()[0] + ax2.get_legend_handles_labels()[0]
        ax.legend(lines, [l.get_label() for l in lines], loc="upper right", fontsize=8, frameon=False)
axes[0].set_ylabel("Messages on the job queue")
fig.tight_layout()
fig.savefig(png)
print(png)
