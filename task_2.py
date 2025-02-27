from datasketch import HyperLogLog
import time
import ujson
import mmap
import os


def log_lines_generator(logs, chunk_size=8192):
    with open(logs, "r", buffering=chunk_size) as fh:
        for line in fh:
            yield line.rstrip()  # Читає рядки на льоту, не навантажуючи пам'ять


def count_logs(logs):
    count_set = set()
    for line in log_lines_generator(logs):
        try:
            data = ujson.loads(line)
            count_set.add(data["remote_addr"])
        except (ValueError, KeyError):
            continue
    return len(count_set)


def count_logs_HLL(logs, p=14):
    hll = HyperLogLog(p)
    batch_ips = set()  # Тимчасовий сет для пакетного оновлення HLL

    file_size = os.path.getsize(logs)
    use_mmap = file_size > 100 * 1024 * 1024  # Використовуємо mmap, якщо файл >100MB

    if use_mmap:
        with open(logs, "r") as fh:
            with mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                for line in iter(mm.readline, b""):
                    try:
                        data = ujson.loads(line.rstrip())
                        batch_ips.add(data["remote_addr"])
                        if len(batch_ips) > 5000:  # Оновлюємо HLL пакетами по 5000
                            for ip in batch_ips:
                                hll.update(ip.encode("utf-8"))
                            batch_ips.clear()
                    except (ValueError, KeyError):
                        continue
    else:
        for line in log_lines_generator(logs):
            try:
                data = ujson.loads(line)
                batch_ips.add(data["remote_addr"])
                if len(batch_ips) > 5000:
                    for ip in batch_ips:
                        hll.update(ip.encode("utf-8"))
                    batch_ips.clear()
            except (ValueError, KeyError):
                continue

    # Оновлюємо залишкові IP-адреси
    for ip in batch_ips:
        hll.update(ip.encode("utf-8"))

    return int(hll.count())


if __name__ == "__main__":
    log_file = "./logs/lms-stage-access.log"

    start_time = time.perf_counter()
    unique_count_set = count_logs(log_file)
    time_set = time.perf_counter() - start_time

    start_time = time.perf_counter()
    unique_count_HLL = count_logs_HLL(log_file)
    time_hll = time.perf_counter() - start_time

    relative_error = (
        abs(unique_count_HLL - unique_count_set) / unique_count_set * 100
        if unique_count_set > 0
        else 0
    )

    print("\nResults Comparison:")
    print(f"{'-' * 85}")
    print(
        f"| {'Method':<20} | {'Unique Count':<15} | {'Time (s)':<15} | {'Relative Error (%)':<15} |"
    )
    print(f"{'-' * 85}")
    print(
        f"| {'Set':<20} | {unique_count_set:<15} | {time_set:<15.8f} | {'0.00':<15} |"
    )
    print(
        f"| {'HyperLogLog':<20} | {unique_count_HLL:<15} | {time_hll:<15.8f} | {relative_error:<15.2f} |"
    )
    print(f"{'-' * 85}")
