#pip install dnspython

from concurrent.futures import ThreadPoolExecutor, as_completed
import dns.reversename
import dns.resolver
from functools import lru_cache

DNS_RESOLVER = dns.resolver.Resolver(configure=False).nameservers = ['1.1.1.1', '1.0.0.1']

@lru_cache(maxsize=10_000)
def ptr_lookup2(ip: str, timeout: float = 1.6) -> str | None:
    if not isinstance(ip, str):
        print(f"НЕ СТРОКА! type={type(ip)}, value={ip!r}")
        return None

    ip = ip.strip()  # убираем пробелы по краям — часто спасает
    if not ip:
        print("Пустая строка после strip")
        return None

    # Простейшая проверка формата IPv4
    parts = ip.split(".")
    if len(parts) != 4 or not all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
        print(f"Некорректный IPv4: {ip!r}")
        return None

    try:
        rev_name = dns.reversename.from_address(ip)
        answers = dns.resolver.resolve(rev_name, "PTR", raise_on_no_answer=False)
        if answers:
            return str(answers[0].target).rstrip('.')
        return None
    except Exception as e:
        print(f"Ошибка для {ip}: {type(e).__name__}: {e}")
        return None


from typing import Optional
from functools import lru_cache
import dns.reversename
import dns.resolver

@lru_cache(maxsize=20_000)
def ptr_lookup3(ip: str | None) -> Optional[str]:
    if not isinstance(ip, str) or not ip:
        return None

    ip_clean = ip.strip()
    if not ip_clean:
        return None

    # Очень быстрая проверка IPv4
    try:
        parts = ip_clean.split(".")
        if len(parts) != 4:
            return None
        for p in parts:
            if not (p.isdigit() and 0 <= int(p) <= 255):
                return None
    except:
        return None

    try:
        rev_name = dns.reversename.from_address(ip_clean)
        answers = dns.resolver.resolve(rev_name, "PTR", raise_on_no_answer=False)
        if answers:
            return str(answers[0].target).rstrip('.')
        return None
    except dns.exception.SyntaxError:
        # print(f"Malformed IP skipped: {ip!r}")   # раскомментировать при отладке
        return None
    except Exception as e:
        # print(f"Другая ошибка для {ip_clean}: {e}")
        return None

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
                result = future.result()
                results[ip] = result
            except Exception:
                results[ip] = None
    return results


def main():
    # Пример с большим количеством повторов
    ips = [
        '5.255.255.242', "178.248.234.61", "185.88.181.8",
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