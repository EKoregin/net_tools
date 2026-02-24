from concurrent.futures import ThreadPoolExecutor, as_completed
import dns.reversename
import dns.resolver
from functools import lru_cache


@lru_cache(maxsize=10_000)   # можно поставить 50_000 или больше, если памяти хватает
def ptr_lookup(ip: str, timeout: float = 1.6) -> str | None:
    try:
        rev_name = dns.reversename.from_address(ip)
        answers = dns.resolver.resolve(rev_name, "PTR", raise_on_no_answer=False)
        if answers:
            return str(answers[0].target).rstrip('.')  # убираем точку в конце
        return None
    except Exception:
        return None


def mass_reverse_dns(ips: list[str], max_workers: int = 400) -> dict:
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ip = {executor.submit(ptr_lookup, ip): ip for ip in ips}
        for future in as_completed(future_to_ip):
            ip = future_to_ip[future]
            try:
                results[ip] = future.result()
            except Exception:
                results[ip] = None
    return results


def main():
    # Пример с большим количеством повторов
    ips = [
        "5.255.255.242", "178.248.234.61", "185.88.181.8",
        "64.233.162.102", "64.233.162.99",
        "204.79.197.200", "72.21.215.200",
        "8.8.8.8", "1.1.1.1", "9.9.9.9",
    ]

    start = time.time()
    result = mass_reverse_dns(ips, max_workers=120)
    elapsed = time.time() - start

    print(f"Обработано {len(ips)} запросов за {elapsed:.2f} сек "
          f"({len(ips)/elapsed:.0f} ip/сек)\n")

    # Выводим только уникальные результаты для наглядности
    unique = {ip: name for ip, name in sorted(result.items())}
    for ip, name in unique.items():
        print(f"{ip:16} → {name or '—'}")


if __name__ == "__main__":
    import time
    main()