from datasketch import HyperLogLog
import time
import json


def count_logs(logs):
    count_set = set()

    with open(file, "r") as fh:
        for line in fh:
            try:
                data = json.loads(line.strip())
                ips = data.get("remote_addr")
                if ips:
                    count_set.add(ips)
            except json.JSONDecodeError as e:
                print(f"Invalid JSON line skipped: {line.strip()} | Error: {e}")
    return len(count_set)


def count_logs_HLL(logs, p=15):
    hll = HyperLogLog(p)

    with open(logs, "r") as fh:
        for line in fh:
            try:
                data = json.loads(line.strip())
                ips = data.get("remote_addr")
                if ips:
                    hll.update(ips.strip().encode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"Invalid JSON line skipped: {line.strip()} | Error: {e}")

    return hll.count()


if __name__ == "__main__":
    file = "./logs/lms-stage-access.log"

    start_time = time.perf_counter()
    unique_count_1 = count_logs(file)
    end_time = time.perf_counter()
    time_set = end_time - start_time

    start_time = time.perf_counter()
    unique_count_HLL = count_logs_HLL(file)
    end_time = time.perf_counter()
    time_hll = end_time - start_time

    print("\nResults Comparison:")
    print(f"{'-' * 65}")
    print(f"| {'Method':<20} | {'Unique Count':<15} | {'Time (s)':<15} |")
    print(f"{'-' * 65}")
    print(f"| {'Set':<20} | {unique_count_1:<15} | {time_set:<15.8f} |")
    print(f"| {'HyperLogLog':<20} | {unique_count_HLL:<15.4f} | {time_hll:<15.8f} |")
    print(f"{'-' * 65}")
