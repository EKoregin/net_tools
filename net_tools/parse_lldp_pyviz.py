# -*- coding: utf-8 -*-
"""
Скрипт для сбора LLDP-топологии с Huawei через Nornir + Netmiko + Pyvis
"""

import re
from typing import List, Dict, Tuple, Optional

from pyvis.network import Network
from nornir import InitNornir
from nornir.core.plugins.inventory import InventoryPluginRegister
from nornir.plugins.inventory.simple import SimpleInventory
from nornir_netmiko.tasks import netmiko_send_command


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


# ────────────────────────────────────────────────────────────────
# Новая реализация отрисовки через pyvis
# ────────────────────────────────────────────────────────────────

DEFAULT_PYVIS_STYLE = {
    "height": "850px",
    "width": "100%",
    "bgcolor": "#ffffff",
    "font_color": "#333333",
    "node_shape": "box",
    "node_color": {
        "background": "#e3f2fd",
        "border": "#1976d2",
        "highlight": {"background": "#bbdefb", "border": "#0d47a1"}
    },
    "edge_color": "#1976d2",
    "edge_arrows": "to",
    "edge_font_size": 11,
    "physics": False,               # включена физика (можно выключить для больших сетей)
    "layout": "force",             # или "hierarchical" для иерархического вида
}


def collect_and_draw_topology(
    nr,
    output_file: str = "huawei_lldp_topology.html",
    style_config: Optional[Dict] = None,
) -> None:
    """
    Собирает топологию из LLDP и рисует интерактивную карту с помощью pyvis.
    Результат сохраняется в .html файл.
    """
    style = DEFAULT_PYVIS_STYLE.copy()
    if style_config:
        style.update(style_config)

    # Создаём сеть
    net = Network(
        height=style["height"],
        width=style["width"],
        bgcolor=style["bgcolor"],
        font_color=style["font_color"],
        directed=True,
        notebook=False,
    )

    net.show_buttons(filter_=['physics']) #Выводит панель настройки физики

    # Настраиваем физику и layout
    if style["layout"] == "hierarchical":
        net.set_layout_hierarchical(
            direction="LR",
            sortMethod="directed",
            levelSeparation=150,
            nodeSpacing=180,
        )
    else:
        net.force_atlas_2based(
            gravity=-50,
            central_gravity=0.01,
            spring_length=120,
            spring_strength=0.08,
            damping=0.4,
        )

    topology: Dict[str, List[Tuple[str, str, str]]] = {}
    seen_edges = set()

    results = nr.run(
        task=netmiko_send_command,
        command_string="display lldp neighbor brief"
    )

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

        # Добавляем узел устройства (если ещё не добавлен)
        if host not in net.nodes:
            net.add_node(
                host,
                label=host,
                title=f"Устройство: {host}",
                shape=style["node_shape"],
                color=style["node_color"],
            )

        for n in neighbors:
            neigh = n["neighbor_dev"].split('.')[0].strip()
            local_p = n["local_intf"]
            remote_p = n["neighbor_intf"]

            topology[host].append((neigh, local_p, remote_p))

            # Добавляем узел соседа, если его ещё нет
            if neigh not in net.nodes:
                net.add_node(
                    neigh,
                    label=neigh,
                    title=f"Устройство: {neigh}",
                    shape=style["node_shape"],
                    color=style["node_color"],
                )

            edge_key = tuple(sorted([host, neigh]) + [local_p, remote_p])
            if edge_key in seen_edges:
                continue
            seen_edges.add(edge_key)

            label = f"{local_p} → {remote_p}"
            net.add_edge(
                source=host,
                to=neigh,
                title=label,
                label=label,
                color=style["edge_color"],
                arrows=style["edge_arrows"],
                font={"size": style["edge_font_size"]},
            )

    if not seen_edges:
        print("Связей не найдено")
        return

    # Сохраняем в HTML
    net.write_html(output_file)
    print(f"\nИнтерактивная топология сохранена: {output_file}")
    print("Откройте файл в браузере для просмотра (можно масштабировать, перетаскивать узлы и т.д.)")

    # Вывод текстового списка (оставлен без изменений)
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
    collect_and_draw_topology(nr)


if __name__ == "__main__":
    main()