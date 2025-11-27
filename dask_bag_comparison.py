import time
from collections import Counter
import urllib.request
from pathlib import Path
import re
from dask.bag import from_sequence


def load_shakespeare():
    url = "https://www.gutenberg.org/files/100/100-0.txt"
    path = Path("shakespeare.txt")
    if not path.exists():
        print("Завантажуємо Шекспіра (5.5 МБ)...")
        path.write_bytes(urllib.request.urlopen(url).read())
    else:
        print("Шекспір вже є — пропускаємо завантаження")

    text = path.read_text(encoding="utf-8")
    return text.splitlines() * 100  # 5.5GB


def sequential(lines):
    print("\nПослідовний Python:")
    start = time.time()
    counter = Counter()
    for line in lines:
        words = re.findall(r'\b[a-zA-Z]+\b', line.lower())
        counter.update(words)
    top_word, top_cnt = counter.most_common(1)[0]
    t = time.time() - start
    print(f"Найчастіше слово: '{top_word}' — {top_cnt:,} разів")
    print(f"Час: {t:.2f} секунд")
    return t


def dask_bag_version(lines):
    print("\nDask Bag (threaded):")
    start = time.time()

    bag = (from_sequence(lines, npartitions=64)
           .map(lambda l: re.findall(r'\b[a-zA-Z]+\b', l.lower()))
           .flatten()
           .frequencies()
           .topk(10, key=1))

    top10 = bag.compute()
    t = time.time() - start

    print("Топ-10 слів:")
    for w, c in top10:
        print(f"   {w} → {c:,}")
    print(f"Час Dask Bag: {t:.2f} секунд")
    return t


if __name__ == "__main__":
    lines = load_shakespeare()
    print(f"Обробляємо {len(lines):,} рядків (~5.5 ГБ в пам'яті)")
    seq = sequential(lines)
    dask = dask_bag_version(lines)
    print(f"ПРИСКОРЕННЯ: {seq / dask:.1f}×")
