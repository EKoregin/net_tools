#!/usr/bin/env python3
"""
Генератор service-set объектов для Huawei USG
на основе списка портов в формате CSV
"""

import csv
import sys
from typing import List, Tuple


def parse_port_range(port_str: str) -> Tuple[int, int]:
    """
    '9200'      → (9200, 9200)
    '21114-21119' → (21114, 21119)
    """
    if '-' in port_str:
        start, end = port_str.split('-', 1)
        return int(start.strip()), int(end.strip())
    else:
        port = int(port_str.strip())
        return port, port


def normalize_protocol(proto: str) -> List[str]:
    """TCP/UDP → ['tcp', 'udp'], TCP → ['tcp'], UDP → ['udp']"""
    proto = proto.strip().upper()
    if proto == 'TCP/UDP':
        return ['tcp', 'udp']
    elif proto == 'TCP':
        return ['tcp']
    elif proto == 'UDP':
        return ['udp']
    else:
        print(f"Предупреждение: неизвестный протокол '{proto}'", file=sys.stderr)
        return []


def generate_service_set(name: str, proto_list: List[str], start: int, end: int) -> List[str]:
    lines = [f"ip service-set {name} type object"]

    for i, proto in enumerate(proto_list):
        lines.append(
            f" service {i} protocol {proto} "
            f"source-port 0 to 65535 "
            f"destination-port {start} to {end}"
        )

    return lines


def main():
    # Можно передать путь к файлу как аргумент или захардкодить
    # filename = sys.argv[1] if len(sys.argv) > 1 else "ports.csv"
    filename = "fw_services.csv"  # ← поменяйте при необходимости

    print("#" * 65)
    print("#  Автосгенерированная конфигурация service-set для Huawei USG")
    print("#" * 65)
    print()

    try:
        with open(filename, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # пропускаем заголовок Protocol,Port

            for row in reader:
                if len(row) < 2:
                    continue

                proto_str, port_str = row[0].strip(), row[1].strip()

                if not port_str:
                    continue

                protocols = normalize_protocol(proto_str)
                if not protocols:
                    continue

                try:
                    start, end = parse_port_range(port_str)
                except ValueError:
                    print(f"Ошибка парсинга диапазона: {port_str!r}", file=sys.stderr)
                    continue

                # Формируем имя объекта
                # Если TCP, то tcp-8000
                # Если UDP, то udp-8000
                # Если TCP/UDP, то 8000
                name = port_str.replace(" ", "")

                if proto_str == "TCP":
                    name = "tcp-" + name
                elif proto_str == "UDP":
                    name = "udp-" + name

                config_lines = generate_service_set(name, protocols, start, end)

                print("\n".join(config_lines))
                print()

    except FileNotFoundError:
        print(f"Файл {filename} не найден", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()