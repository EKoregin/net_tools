# -*- coding: utf-8 -*-
"""
Скрипт для сбора LLDP-топологии с Huawei через Nornir + Netmiko + D2
Скрипт создает d2 файл и генерирует картинку в png.
D2 файл можно затем открыть с помощью d2 утилиты и смотреть в веб-браузере
d2 --layout=elk -w huawei_lldp_topology.d2 out.svg
Если d2 не установлен, то установить (Нужен предварительно уст. GO)
go install oss.terrastruct.com/d2@latest
SVG icons
https://d2lang.com/tour/icons/
"""

import os
import re
from typing import List, Dict, Tuple, Optional

from nornir import InitNornir
from nornir.core.plugins.inventory import InventoryPluginRegister
from nornir.plugins.inventory.simple import SimpleInventory
from nornir_netmiko.tasks import netmiko_send_command
import subprocess


# ────────────────────────────────────────────────────────────────
# Функции парсинга LLDP — без изменений
# ────────────────────────────────────────────────────────────────

def parse_huawei_lldp_brief(output: str) -> List[Dict[str, str]]:
    lines = [line.rstrip() for line in output.splitlines() if line.strip()]

    if not lines:
        return []

    start_idx = _find_data_start(lines)
    if start_idx == -1:
        return []

    result = []
    for line in lines[start_idx:]:
        if not line.strip() or re.match(r'^-{3,}$', line.strip()):
            continue

        entry = _parse_data_line(line)
        if entry:
            result.append(entry)

    return result


def _find_data_start(lines: List[str]) -> int:
    for i, line in enumerate(lines):
        lower = line.lower()
        if any(word in lower for word in ['local intf', 'local interface', 'neighbor dev', 'neighbor device']):
            for j in range(i + 1, min(i + 6, len(lines))):
                if re.match(r'^-{3,}$', lines[j].strip()) or not lines[j].strip():
                    continue
                if _looks_like_data_line(lines[j]):
                    return j
            return i + 1
    return 0 if _looks_like_data_line(lines[0]) else -1


def _looks_like_data_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if re.match(r'^[A-Za-z0-9/-]+[0-9/]+', stripped):
        if any(c.isdigit() for c in stripped):
            return True
    return False


def _parse_data_line(line: str) -> Optional[Dict[str, str]]:
    m = re.match(
        r'^(\S+?)\s{2,}(.+?)\s{2,}(\S+?)\s{2,}(\d+)\s*$', line
    )
    if m:
        local, dev, neigh, exp = m.groups()
        return _make_entry(local, dev, neigh, exp)

    m = re.match(
        r'^(\S+?)\s{2,}(\d+)\s{2,}(\S+?)\s{2,}(.+?)\s*$', line
    )
    if m:
        local, exp, neigh, dev = m.groups()
        return _make_entry(local, dev, neigh, exp)

    parts = re.split(r'\s{2,}', line.strip())
    cleaned = [p.strip() for p in parts if p.strip()]

    if len(cleaned) < 3:
        return None

    exp_candidates = [p for p in cleaned if p.isdigit() and 1 <= len(p) <= 3]
    if exp_candidates:
        exp = exp_candidates[-1]
        idx = cleaned.index(exp)
        if idx == 1:
            local = cleaned[0]
            neigh = cleaned[2]
            dev = ' '.join(cleaned[3:]) if len(cleaned) > 3 else cleaned[2]
        else:
            local = cleaned[0]
            neigh = cleaned[-2] if len(cleaned) > 3 else cleaned[-1]
            dev = ' '.join(cleaned[1:idx])
    else:
        local = cleaned[0]
        neigh = cleaned[-2]
        dev = ' '.join(cleaned[1:-1])
        exp = ''

    return _make_entry(local, dev, neigh, exp)


def _make_entry(local: str, dev: str, neigh: str, exp: str = '') -> Dict[str, str]:
    return {
        'local_intf': local.strip(),
        'neighbor_dev': re.sub(r'\s+', ' ', dev.strip()),
        'neighbor_intf': neigh.strip(),
        'exptime': exp.strip(),
    }



def collect_and_draw_topology(
    nr,
    output_file: str = "huawei_lldp_topology",      # без расширения
    open_image: bool = True,
    save_outputs: bool = False,
    use_saved: bool = False,
    output_dir: str = "device_outputs",
) -> None:
    """
    Собирает топологию из LLDP и генерирует диаграмму в формате D2 → PNG/SVG
    """
    topology: Dict[str, List[Tuple[str, str, str]]] = {}
    seen_edges = set()

    ##############################################################
    ## Блок сохранения ###########################################
    class FakeResult:
        def __init__(self, failed: bool, result: str = None, exception: str = None):
            self.failed = failed
            self.result = result
            self.exception = exception

    if use_saved:
        results = {}
        os.makedirs(output_dir, exist_ok=True)  # Ensure dir exists, though not necessary for reading
        for host in nr.inventory.hosts:
            file_path = os.path.join(output_dir, f"{host}.txt")
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    raw = f.read()
                results[host] = FakeResult(failed=False, result=raw)
            else:
                print(f"Файл для {host} не найден: {file_path}")
                results[host] = FakeResult(failed=True, exception='Файл не найден')
    else:
        results = nr.run(
            task=netmiko_send_command,
            command_string="display lldp neighbor brief"
        )

    if save_outputs and not use_saved:
        os.makedirs(output_dir, exist_ok=True)
        for host, r in results.items():
            if not r.failed:
                file_path = os.path.join(output_dir, f"{host}.txt")
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(r.result)
                print(f"Вывод для {host} сохранен в {file_path}")

    ##################################################################################

    d2_lines = [
        "# LLDP Topology - Huawei devices",
        "direction: down",               # или down / left / up
        "",
        "# Глобальные стили (очень гибко настраивается)",
        "classes: {",
        "  device: {",
        '    shape: "rectangle"',
        "    style.fill: lightblue",
        "    style.stroke: steelblue",
        "    style.stroke-width: 2",
        "    style.font-size: 14",
        "  }",
        "  link: {",
        "    style.stroke: darkblue",
        "    style.stroke-width: 2",
        "    style.font-size: 11",
        "  }",
        "}",
        "",
    ]

    for host, r in results.items():
        if r.failed:
            print(f"[{host}] Ошибка: {r.exception or 'неизвестная'}")
            continue

        raw = r.result
        neighbors = parse_huawei_lldp_brief(raw)

        if not neighbors:
            print(f"[{host}] → соседей не найдено")
            continue

        print(f"[{host}] → {len(neighbors)} соседей")

        topology[host] = []

        # Добавляем узел устройства
        clean_host = host.replace("-", "_").replace(".", "_")  # D2 не любит дефисы в ключах

        icons_path = "./icons"
        ICON_MAP = {
            "spine": f"{icons_path}/switch2.svg",
            "aggregation": f"{icons_path}/l3switch2.svg",
            "agg": f"{icons_path}/l3switch2.svg",
            "access": f"{icons_path}/switch2.svg",
            "leaf": f"{icons_path}/switch2.svg",
        }

        role = "device"  # default
        icon = ICON_MAP.get("access", f"{icons_path}/switch.svg")

        for keyword, assigned_role in [
            ("spine", "spine"),
            ("agg", "aggregation"),
            ("access", "access"),
        ]:
            if keyword in host.lower():
                role = assigned_role
                icon = ICON_MAP.get(assigned_role, icon)
                break

        node_line = f'{clean_host}: "{host}" {{'
        if icon:
            node_line += '\n  shape: image'
            node_line += f'\n  icon: {icon}'
        else:
            node_line += f'\n  class: {role}'
        node_line += '\n}'

        d2_lines.append(node_line)

        for n in neighbors:
            neigh_raw = n["neighbor_dev"]
            neigh = neigh_raw.split('.')[0].strip()
            local_p = n["local_intf"]
            remote_p = n["neighbor_intf"]

            # clean_neigh = neigh.replace("-", "_").replace(".", "_")
            clean_neigh = neigh

            topology[host].append((neigh, local_p, remote_p))

            edge_key = tuple(sorted([host, neigh]) + sorted([local_p, remote_p]))
            if edge_key in seen_edges:
                continue
            seen_edges.add(edge_key)

            label = f"{local_p} → {remote_p}"
            # В D2 связь записывается как undirected
            d2_lines.append(f'{clean_host} -> {clean_neigh}: "{label}" {{ class: link }}')


    if not seen_edges:
        print("Связей не найдено")
        return

    # Финальные настройки (можно сильно кастомизировать)
    # d2_lines.extend([
    #     "",
    #     "# Дополнительные улучшения раскладки",
    #     "layout: elk",                    # elk / dagre / tala — разные движки
    #     "theme: Neutral Default",         # или Neutral Grey, Flagship, etc.
    #     "",
    # ])

    # Записываем .d2 файл
    d2_path = f"{output_file}.d2"
    png_path = f"{output_file}.png"

    with open(d2_path, "w", encoding="utf-8") as f:
        f.write("\n".join(d2_lines))

    print(f"D2-файл сгенерирован: {d2_path}")

    # Рендерим в PNG (требует установленного d2 в PATH)
    try:
        subprocess.run(
            ["d2", "--layout=elk", d2_path, png_path],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"\nДиаграмма сохранена: {png_path}")

        if open_image:
            if os.name == 'nt':  # Windows
                os.startfile(png_path)
            else:
                subprocess.run(["open" if os.name == 'posix' else "xdg-open", png_path])

    except FileNotFoundError:
        print("Ошибка: команда 'd2' не найдена. Установите D2: https://d2lang.com/tour/install")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка рендера D2:\n{e.stderr}")

    # Текстовый вывод связей (без изменений)
    print("\nСвязи:")
    for dev in sorted(topology):
        if topology[dev]:
            print(f"  {dev}:")
            for neigh, lp, rp in sorted(topology[dev]):
                print(f"    • {lp:20} → {neigh} ({rp})")

def main():
    InventoryPluginRegister.register("SimpleInventory", SimpleInventory)

    nr = InitNornir(
        inventory={
            "plugin": "SimpleInventory",
            "options": {
                "host_file": "hosts.yaml",
            }
        },
        logging={"enabled": False}
    )

    print("=== Сбор LLDP-топологии с Huawei (Nornir + Netmiko + Pyvis) ===\n")
    collect_and_draw_topology(
        nr,
        save_outputs=True,  # Сохранить выводы в файлы
        use_saved=True,    # Использовать сохраненные файлы (если True, не подключается к устройствам)
        output_dir="device_outputs"
    )


if __name__ == "__main__":
    main()