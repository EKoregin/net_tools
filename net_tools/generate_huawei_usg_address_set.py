#!/usr/bin/env python3
"""
Скрипт для генерации конфигурации address-set для Huawei USG из CSV-файла.

Формат входного CSV:
Name;IP
ha-cluster-ip;169.254.0.0/29
zabbix3.whs.wb.ru;10.117.253.44
DC_public_IP;"185.62.200.99\n185.62.203.148\n..."

Правила:
- Если в IP указана подсеть (/mask) → используем network mask
- Если указан одиночный IP без / → добавляем mask 32
- IP-адреса могут быть перечислены через перенос строки в одной ячейке
- Имена address-set берутся из колонки Name
- Пустые строки и лишние пробелы обрабатываются
"""

import csv
import re
import sys
from pathlib import Path
from typing import Dict, List


def is_valid_network(value: str) -> bool:
    """Проверяет, выглядит ли строка как сеть IPv4 с маской (CIDR)"""
    return bool(re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}$', value.strip()))


def is_valid_ip(value: str) -> bool:
    """Проверяет, выглядит ли строка как одиночный IPv4-адрес"""
    return bool(re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', value.strip()))


def parse_ip_line(line: str) -> List[tuple[str, str]]:
    """
    Разбирает одну строку IP-адресов (может содержать несколько через \n)
    Возвращает список кортежей (network, mask)
    """
    results = []
    # Разделяем по переносам строк и чистим
    entries = [e.strip() for e in line.split('\n') if e.strip()]

    for entry in entries:
        entry = entry.strip('"').strip()  # убираем кавычки, если были

        if not entry:
            continue

        if '/' in entry:
            # CIDR-формат
            if is_valid_network(entry):
                network, mask = entry.split('/')
                results.append((network.strip(), mask.strip()))
            else:
                print(f"Некорректный CIDR: {entry}", file=sys.stderr)
        else:
            # Одиночный IP → mask 32
            if is_valid_ip(entry):
                results.append((entry, "32"))
            else:
                print(f"Некорректный IP-адрес: {entry}", file=sys.stderr)

    return results


def main(csv_path: str):
    # Собираем все address-set в словарь: имя → список (network, mask)
    address_sets: Dict[str, List[tuple[str, str]]] = {}

    with open(csv_path, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')

        for row in reader:
            name = row.get('Name', '').strip()
            ip_column = row.get('IP', '').strip()

            if not name or not ip_column:
                continue

            # Парсим все IP для этого имени
            networks = parse_ip_line(ip_column)

            if networks:
                if name not in address_sets:
                    address_sets[name] = []
                address_sets[name].extend(networks)

    # Генерируем конфигурацию
    print("### Сгенерированная конфигурация address-set ###\n")

    for name, entries in sorted(address_sets.items()):
        print(f'ip address-set {name} type object')

        # Нумерация с 0 (как в вашем примере)
        for i, (network, mask) in enumerate(entries, start=0):
            print(f' address {i} {network} mask {mask}')
        print()

    print(f"Всего создано address-set: {len(address_sets)}")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Использование: python generate_huawei_address_sets.py input.csv")
        sys.exit(1)

    csv_file = sys.argv[1]
    if not Path(csv_file).is_file():
        print(f"Файл не найден: {csv_file}", file=sys.stderr)
        sys.exit(1)

    main(csv_file)