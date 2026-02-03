#!/usr/bin/env python3
"""
Скрипт парсит CSV файл лога с Huawei USG с IP адресами и для каждого IP добавляет описание (название VLAN если есть),
которое находит в Netbox.
Из CSV-файла берет поля:
SrcAddr, DstAddr, Port, Protocol
Если поиск в Netbox находит несколько сетей, то выбирается самый длинный префикс.

Формат файла .env
NETBOX_URL = "https://netbox.domain.com"
TOKEN = "e998dklsdf987fsljdsf99798lsdf979j"
TENANT = "wh-berlin"

Результат выводится в консоль и в .csv файл
"""
import ipaddress
import os
from typing import Dict, Optional

import pandas as pd
import pynetbox
from dotenv import load_dotenv

# ────────────────────────────────────────────────
# НАСТРОЙКИ
load_dotenv()
NETBOX_URL  = os.getenv("NETBOX_URL")
TOKEN       = os.getenv("TOKEN")
TENANT      = os.getenv("TENANT")
CSV_DIR     = "csv"
INPUT_FILE  = CSV_DIR + "/policy_org.csv"
OUTPUT_FILE = CSV_DIR + "/unique_with_counts.csv"

# Список полей, которые нужно оставить
# Порядок в списке определяет порядок колонок в выводе
# Первое поле будет использовано для сортировки
NEEDED_COLUMNS = [
    "Source Address",
    "Destination Address",
    "Destination Port",
    "Protocol",
]

Prefixes = set()                    # Кэш всех найденных префиксов
ip_to_prefix: Dict[str, str] = {}             # Кэш: IP → самый длинный префикс

def init_netbox():
    """Инициализация подключения к NetBox"""
    try:
        nb = pynetbox.api(
            url=NETBOX_URL,
            token=TOKEN,
            threading=True,           # ускоряет множественные запросы (если много IP)
        )
        nb.http_session.verify = True  # можно поставить False для self-signed сертификатов (не рекомендуется)
        # nb.http_session.timeout = 10
        print("Подключение к NetBox установлено")
        return nb
    except Exception as e:
        print(f"Ошибка подключения к NetBox: {e}")
        return None

def get_longest_prefix(ip_str: str, nb) -> tuple[Optional[str], Optional[str]]:
    """Возвращает самый длинный префикс для IP (из кэша или NetBox)"""
    if not ip_str or ip_str.lower() in ("nan", "none", "", "0.0.0.0"):
        return None, None

    ip_str = ip_str.strip()

    # 1. Уже есть в кэше?
    if ip_str in ip_to_prefix:
        prefix, description = ip_to_prefix[ip_str]
        return prefix, description

    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        print(f"⚠️ Некорректный IP: {ip_str}")
        ip_to_prefix[ip_str] = (None, None)
        return None, None

    # 2. Ищем среди уже известных префиксов (самый длинный)
    best_prefix = None
    best_len = -1
    best_description = None

    for prefix_str, description in Prefixes:
        try:
            net = ipaddress.ip_network(prefix_str, strict=False)
            if ip in net and net.prefixlen > best_len:
                best_len = net.prefixlen
                best_prefix = prefix_str
                best_description = description
        except ValueError:
            continue

    if best_prefix:
        ip_to_prefix[ip_str] = (best_prefix, best_description)
        return best_prefix, best_description

    # 3. Запрос в NetBox через pynetbox
    try:
        # Ищем префиксы, содержащие данный IP
        prefixes = nb.ipam.prefixes.filter(
            contains=ip_str,
            tenant=TENANT,
            limit=100
        )

        if not prefixes:
            ip_to_prefix[ip_str] = (None, None)
            return None, None

        # Находим самый длинный (max prefixlen)
        longest = list(prefixes)[-1]
        prefix_str = str(longest.prefix)  # "10.10.5.0/24"
        description = str(longest.vlan)

        Prefixes.add((prefix_str, description))
        ip_to_prefix[ip_str] = (prefix_str, description)
        return prefix_str, description

    except Exception as e:
        print(f"❌ Ошибка NetBox для {ip_str}: {e}")
        ip_to_prefix[ip_str] = None, None
        return None, None

# ────────────────────────────────────────────────

def main():
    nb = init_netbox()

    def get_prefix_and_descr(search_ip):
        p, d = get_longest_prefix(search_ip, nb)
        return p, d


    try:
        # Читаем CSV, все поля как строки
        df = pd.read_csv(INPUT_FILE, dtype=str)

        # Проверяем наличие всех нужных столбцов
        missing = [col for col in NEEDED_COLUMNS if col not in df.columns]
        if missing:
            print("Ошибка: в файле отсутствуют следующие поля:")
            print(", ".join(missing))
            return

        # Оставляем только нужные столбцы
        df = df[NEEDED_COLUMNS].copy()

        unique_src_ips = df["Source Address"].dropna().unique()
        print(f"Уникальных Source Address для поиска префиксов: {len(unique_src_ips)}")

        # Заполняем кэш префиксов
        for ip in unique_src_ips:
            get_longest_prefix(ip, nb)

        print(f"Найдено уникальных префиксов: {len(Prefixes)}")

        # Добавляем колонку SrcPrefix и SrcDescription
        prefix_descr = df["Source Address"].apply(get_prefix_and_descr)
        df["SrcPrefix"] = prefix_descr.apply(lambda x: x[0])
        df["SrcDescription"] = prefix_descr.apply(lambda x: x[1])

        group_cols = NEEDED_COLUMNS + ["SrcPrefix", "SrcDescription"]
        # Подсчитываем количество вхождений каждой уникальной комбинации
        counts = df.groupby(group_cols, dropna=False).size().reset_index(name='Count')

        # Сортируем по первому полю (или можно по Count descending, если нужно)
        sort_column = NEEDED_COLUMNS[0]
        counts = counts.sort_values(by=sort_column)

        # Выводим результат
        print("\nУникальные строки и количество их повторений:")
        print("-" * 130)

        # Красивый вывод с выравниванием
        header = "  ".join(f"{col:<18}" for col in group_cols) + "  Count"
        print(header)
        print("-" * 130)

        for _, row in counts.iterrows():
            line = "  ".join(f"{str(row[col]):<18}" for col in group_cols)
            print(f"{line}  {row['Count']}")

        print("-" * 130)
        print(f"Всего уникальных комбинаций: {len(counts)}")
        print(f"Всего строк в исходном файле: {len(df)}")

        # Если хотите сохранить результат в файл
        counts.to_csv(OUTPUT_FILE, index=False)
        print(f"\nСохранено в: {OUTPUT_FILE}")

    except FileNotFoundError:
        print(f"Файл не найден: {INPUT_FILE}")
    except Exception as e:
        print("Ошибка при обработке:")
        print(e)

if __name__ == "__main__":
    main()