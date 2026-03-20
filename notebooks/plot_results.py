# notebooks/plot_results.py  (or paste into Jupyter)
# Done by Neha
import matplotlib.pyplot as plt
import numpy as np

workers     = [1, 2, 3]
runtimes    = [300, 170, 120]   # <-- replace with your actual numbers (seconds)

baseline    = runtimes[0]
speedup     = [baseline / t for t in runtimes]
ideal       = workers  # linear speedup

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

ax1.plot(workers, runtimes, 'bo-', linewidth=2, markersize=8, label="Actual")
ax1.set(xlabel="Number of Workers", ylabel="Runtime (s)",
        title="Runtime vs. Workers", xticks=workers)
ax1.grid(True); ax1.legend()

ax2.plot(workers, speedup, 'ro-', linewidth=2, markersize=8, label="Actual Speedup")
ax2.plot(workers, ideal,   'g--',              linewidth=2,             label="Ideal Linear")
ax2.set(xlabel="Number of Workers", ylabel="Speedup Factor",
        title="Speedup vs. Ideal", xticks=workers)
ax2.grid(True); ax2.legend()

plt.tight_layout()
plt.savefig("docs/scalability_results.png", dpi=150)
plt.show()