import time
from dask import delayed, compute

def square_sum_part(start, end):
    total = 0
    for i in range(start, end):
        total += i * i
    return total


if __name__ == "__main__":
    N = 2_000_000_000
    parts = 64

    print("Послідовний Python:")
    start = time.time()
    seq_result = square_sum_part(1, N + 1)
    seq_time = time.time() - start
    print(f"Результат = {seq_result:,}")
    print(f"Час = {seq_time:.2f} секунд")

    print("\nDask Delayed (з процесами):")
    step = N // parts
    tasks = []

    for i in range(parts):
        s = i * step + 1
        e = (i + 1) * step + 1 if i < parts - 1 else N + 1
        tasks.append(delayed(square_sum_part)(s, e))

    total = delayed(sum)(tasks)

    start = time.time()
    result = total.compute(scheduler='processes')
    dask_time = time.time() - start

    print(f"Результат = {result:,}")
    print(f"Час = {dask_time:.2f} секунд")
    print(f"ПРИСКОРЕННЯ = {seq_time / dask_time:.1f}×")