import numpy as np
import dask.array as da
import time

# Спроба з NumPy – впаде з MemoryError на більшості машин
print("NumPy:")
try:
    start = time.time()
    x_np = np.random.random((50000, 50000))
    mean_np = x_np.mean()
    print(f"NumPy mean = {mean_np:.6f}, час = {time.time() - start:.2f} с")
except MemoryError:
    print("NumPy: MemoryError – масив не вміщується в RAM")

# Dask Array з чанками 5000×5000 (приблизно 200 МБ кожен)
print("\nDask Array:")
start = time.time()
x_da = da.random.random((50000, 50000), chunks=(5000, 5000))
mean_da = x_da.mean()
result = mean_da.compute()                     # тут починається реальне виконання
print(f"Dask mean = {result:.6f}, час = {time.time() - start:.2f} с")
