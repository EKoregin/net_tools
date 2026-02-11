#!/usr/bin/env python3
"""
Скрипт парсит CSV файл лога с Huawei USG, или лог с Fortigate с IP адресами и для каждого IP добавляет описание (название VLAN если есть),
которое находит в Netbox.
Берутся поля:
SrcAddr, DstAddr, Port, Protocol
Если поиск в Netbox находит несколько сетей, то выбирается самый длинный префикс.
Формат файла .env
NETBOX_URL = "https://netbox.domain.com"
TOKEN = "e998dklsdf987fsljdsf99798lsdf979j"

Результат выводится в консоль и в Excel

Запуск
Если запускаете из консоли, то предварительно установите зависимости
Создайте файл requirements.txt с содержимым:
pynetbox~=7.6.1
pandas~=3.0.0
dotenv~=0.9.9
openpyxl~=3.1.5

и запустите
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

Запуск скрипта:
Для Huawei USG
python.exe .\parse_fw_log.py --tenant=Moscow --fw=huawei --file=policy_org.csv
Для Fortigate
python.exe .\parse_fw_log.py --tenant=Moscow --fw=fortigate --file=fortigate.log
"""
import argparse
import ipaddress
import os
import re
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import pynetbox
from dotenv import load_dotenv
from datetime import datetime

# ────────────────────────────────────────────────
# НАСТРОЙКИ
load_dotenv()
NETBOX_URL = os.getenv("NETBOX_URL")
TOKEN = os.getenv("TOKEN")
OUTPUT_DIR = "parse_log_result"

NEEDED_COLUMNS = [
    "Source Address",
    "Destination Address",
    "Destination Port",
    "Protocol",
]

FORTIGATE_PATTERN = re.compile(
    r'srcip=(?P<srcip>[^ ]+)\s+'
    r'.*?'
    r'dstip=(?P<dstip>[^ ]+)\s+'
    r'.*?'
    r'dstport=(?P<dstport>\d+)\s+'
    r'.*?'
    r'proto=(?P<proto>\d+)'
)

Prefixes = set()  # Кэш всех найденных префиксов
ip_to_prefix: Dict[str, str] = {}  # Кэш: IP → самый длинный префикс


def init_netbox():
    """Инициализация подключения к NetBox"""
    try:
        nb = pynetbox.api(
            url=NETBOX_URL,
            token=TOKEN,
            threading=True,  # ускоряет множественные запросы (если много IP)
        )
        nb.http_session.verify = True  # можно поставить False для self-signed сертификатов (не рекомендуется)
        # nb.http_session.timeout = 10
        print("Подключение к NetBox установлено")
        return nb
    except Exception as e:
        print(f"Ошибка подключения к NetBox: {e}")
        return None


def parse_fortigate_log(log_lines):
    """
    Парсит строки лога FortiGate и возвращает DataFrame с нужными полями.
    """
    data = []
    for line in log_lines:
        match = FORTIGATE_PATTERN.search(line)
        if match:
            data.append(match.groupdict())

    if not data:
        print("Не удалось найти подходящие записи в логе FortiGate")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    # Приводим названия колонок к тому же стилю, что и в CSV-варианте
    df = df.rename(columns={
        'srcip': 'Source Address',
        'dstip': 'Destination Address',
        'dstport': 'Destination Port',
        'proto': 'Protocol'
    })
    return df


def process_csv_file(file_path, tenant, nb):
    """
    Обрабатывает CSV-файл: читает, добавляет префиксы и описания, группирует.
    """
    try:
        # Читаем CSV, все поля как строки
        df = pd.read_csv(file_path, dtype=str)

        # Проверяем наличие всех нужных столбцов
        missing = [col for col in NEEDED_COLUMNS if col not in df.columns]
        if missing:
            print("Ошибка: в CSV-файле отсутствуют следующие поля:")
            print(", ".join(missing))
            return None

        # Оставляем только нужные столбцы
        df = df[NEEDED_COLUMNS].copy()

        unique_src_ips = df["Source Address"].dropna().unique()
        print(f"Уникальных Source Address для поиска префиксов: {len(unique_src_ips)}")

        # Заполняем кэш префиксов
        for ip in unique_src_ips:
            get_longest_prefix(ip, nb, tenant)

        print(f"Найдено уникальных префиксов: {len(Prefixes)}")

        # Добавляем колонки SrcPrefix и SrcDescription
        prefix_descr = df["Source Address"].apply(lambda ip: get_longest_prefix(ip, nb, tenant))
        df["SrcPrefix"] = prefix_descr.apply(lambda x: x[0])
        df["SrcDescription"] = prefix_descr.apply(lambda x: x[1])

        # Группируем и считаем количество
        group_cols = NEEDED_COLUMNS + ["SrcPrefix", "SrcDescription"]
        result = df.groupby(group_cols, dropna=False).size().reset_index(name='Count')

        # Сортируем
        sort_column = NEEDED_COLUMNS[0]
        result = result.sort_values(by=sort_column)

        return result, group_cols

    except FileNotFoundError:
        print(f"Файл не найден: {file_path}")
        return None, None
    except Exception as e:
        print("Ошибка при обработке CSV:")
        print(e)
        return None, None


def process_fortigate_log(file_path, tenant, nb):
    """
    Обрабатывает текстовый лог FortiGate.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            log_lines = [line.strip() for line in f if line.strip()]

        df = parse_fortigate_log(log_lines)

        if df.empty:
            return None, None

        # Для лога FortiGate берём другие необходимые колонки
        # Можно адаптировать NEEDED_COLUMNS под лог, либо задать отдельный список
        fortigate_columns = ["Source Address", "Destination Address", "Destination Port", "Protocol"]

        # Проверяем наличие колонок (они уже переименованы)
        missing = [col for col in fortigate_columns if col not in df.columns]
        if missing:
            print("Ошибка: после парсинга лога отсутствуют поля:", ", ".join(missing))
            return None, None

        df = df[fortigate_columns].copy()

        unique_src_ips = df["Source Address"].dropna().unique()
        print(f"Уникальных Source Address для поиска префиксов: {len(unique_src_ips)}")

        # Заполняем кэш префиксов
        for ip in unique_src_ips:
            get_longest_prefix(ip, nb, tenant)

        print(f"Найдено уникальных префиксов: {len(Prefixes)}")

        # Добавляем префикс и описание
        prefix_descr = df["Source Address"].apply(lambda ip: get_longest_prefix(ip, nb, tenant))
        df["SrcPrefix"] = prefix_descr.apply(lambda x: x[0])
        df["SrcDescription"] = prefix_descr.apply(lambda x: x[1])

        # Группируем
        group_cols = fortigate_columns + ["SrcPrefix", "SrcDescription"]
        result = df.groupby(group_cols, dropna=False).size().reset_index(name='Count')

        # Сортируем по первому столбцу
        result = result.sort_values(by=fortigate_columns[0])

        return result, group_cols

    except FileNotFoundError:
        print(f"Файл лога не найден: {file_path}")
        return None, None
    except Exception as e:
        print("Ошибка при обработке лога FortiGate:")
        print(e)
        return None, None


def print_results(result_df, group_cols):
    """
    Красиво выводит результат в консоль.
    """
    if result_df is None or result_df.empty:
        print("Нет данных для вывода")
        return

    print("\nУникальные строки и количество их повторений:")
    print("-" * 130)

    header = "  ".join(f"{col:<18}" for col in group_cols) + "  Count"
    print(header)
    print("-" * 130)

    for _, row in result_df.iterrows():
        line = "  ".join(f"{str(row[col]):<18}" for col in group_cols)
        print(f"{line}  {row['Count']}")

    print("-" * 130)
    print(f"Всего уникальных комбинаций: {len(result_df)}")
    print(f"Всего строк в обработке: {result_df['Count'].sum()}")


def get_longest_prefix(ip_str: str, nb, tenant: str) -> tuple[Optional[str], Optional[str]]:
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
        prefixes = list(nb.ipam.prefixes.filter(
            contains=ip_str,
            # tenant=TENANT,
            limit=100
        ))

        if not prefixes:
            ip_to_prefix[ip_str] = (None, None)
            return None, None

        # Поиск самого длинного префикса и проверка на tenant
        # 1. Ищем префиксы по tenant
        # 2. Если находим несколько, то выбираем самый длинный
        # 3. Если не находим по tenant, то выбираем самый длинный у оставшихся
        search_tenant_prefixes = []
        other_prefixes = []
        # 1. Распределяем префиксы по арендатору (tenant)
        for prefix in prefixes:
            if prefix.tenant is not None and prefix.tenant.name.lower() == tenant.lower():
                search_tenant_prefixes.append(prefix)
            else:
                other_prefixes.append(prefix)

        # 2. Если по искомому арендатору есть префиксы, то выбираемы самый длинные
        # иначе ищем в префиксах других арендаторов.
        # Выбираем самый последний, если длина префиксов одинаковая.
        longest_prefix = None
        if len(search_tenant_prefixes) > 0:
            longest_prefix = max(search_tenant_prefixes, key=lambda p: int(str(p).split("/")[-1]))
        else:
            if len(other_prefixes) > 0:
                longest_prefix = max(other_prefixes, key=lambda p: int(str(p).split("/")[-1]))

        prefix_str = str(longest_prefix.prefix)
        vlan = longest_prefix.vlan.display if longest_prefix.vlan else ""
        descr = longest_prefix.description if longest_prefix.description else ""
        role = longest_prefix.role.display if longest_prefix.role else ""
        tenant = longest_prefix.tenant.display if longest_prefix.tenant else ""

        description = (vlan + "-" if len(vlan) != 0 else descr + " " + role + " ") + tenant

        Prefixes.add((prefix_str, description))
        ip_to_prefix[ip_str] = (prefix_str, description)
        return prefix_str, description

    except Exception as e:
        print(f"❌ Ошибка NetBox для {ip_str}: {e}")
        ip_to_prefix[ip_str] = None, None
        return None, None

def save_results(df: pd.DataFrame, base_name: str = "traffic_analysis"):
    """Сохраняет DataFrame в CSV и Excel"""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    # csv_path = os.path.join(OUTPUT_DIR, f"{base_name}_{timestamp}.csv")
    xlsx_path = os.path.join(OUTPUT_DIR, f"{base_name}_{timestamp}.xlsx")

    try:
        # CSV
        # df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        # print(f"Сохранено в CSV:  {csv_path}")

        # Excel — пробуем openpyxl, если нет → xlsxwriter
        try:
            with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name="Результат", index=False)
            print(f"Сохранено в Excel (openpyxl): {xlsx_path}")
        except ImportError:
            print("openpyxl не найден → пробуем xlsxwriter...")
            try:
                with pd.ExcelWriter(xlsx_path, engine='xlsxwriter') as writer:
                    df.to_excel(writer, sheet_name="Результат", index=False)
                print(f"Сохранено в Excel (xlsxwriter): {xlsx_path}")
            except ImportError:
                print("xlsxwriter тоже не найден → сохраняем только CSV")
                print("Установите один из пакетов: pip install openpyxl   или   pip install xlsxwriter")

    except Exception as e:
        print(f"Ошибка при сохранении файлов: {e}")


def main():
    parser = argparse.ArgumentParser(description="Обработка лога FW, поиск описания для SourceIP")
    parser.add_argument("--fw", required=True, choices=["huawei", "fortigate"],
                        help="Тип FW. Возможны: huawei, fortigate")
    parser.add_argument("--tenant", required=True, help="Имя площадки / tenant в NetBox")
    parser.add_argument("--file", required=True, help="Путь к файлу (CSV или лог)")
    args = parser.parse_args()
    nb = init_netbox()

    result = None
    group_cols = None

    if args.fw == "huawei":
        print(f"Обработка CSV-файла: {args.file}")
        result, group_cols = process_csv_file(args.file, args.tenant, nb)

    elif args.fw == "fortigate":
        print(f"Обработка лога FortiGate: {args.file}")
        result, group_cols = process_fortigate_log(args.file, args.tenant, nb)

    print_results(result, group_cols)
    if result is not None and not result.empty:
        save_results(result)

if __name__ == "__main__":
    main()
