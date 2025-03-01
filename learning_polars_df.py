import numpy as np
import polars as pl

num_rows = 10000
rng = np.random.default_rng(seed=17)
buildings_data = {
     "sqmeter": rng.exponential(scale=130, size=num_rows),
     "year": rng.integers(low=1995, high=2023, size=num_rows),
     "building_type": rng.choice(["A", "B", "C"], size=num_rows),
  }

buildings = pl.DataFrame(buildings_data)
print(buildings)