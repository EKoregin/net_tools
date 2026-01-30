# -*- coding: utf-8 -*-
"""
Скрипт для сбора LLDP-топологии с Huawei через Nornir + Netmiko + Graphviz
Сохраняет созданную диаграмму сети в .dot формат

Читает файл hosts.yaml

Для конвертации из dot в drawio открыть WSL с установленным graphviz2drawio
и запустить
python3 -m graphviz2drawio huawei_lldp_topology.dot -o huawei_lldp_topology.drawio
"""
import os
import re
from typing import List, Dict, Tuple, Optional

from graphviz import Digraph
from nornir import InitNornir
from nornir.core.plugins.inventory import InventoryPluginRegister
from nornir.plugins.inventory.simple import SimpleInventory
from nornir_netmiko.tasks import netmiko_send_command


def parse_huawei_lldp_brief(output: str) -> List[Dict[str, str]]:
    """
    Парсит вывод команды display lldp neighbor(s) brief на Huawei.
    Возвращает список словарей с ключами:
        local_intf, neighbor_dev, neighbor_intf, exptime (может быть пустым)

    Поддерживает разные варианты заголовков и порядок столбцов.
    """
    lines = [line.rstrip() for line in output.splitlines() if line.strip()]

    if not lines:
        return []

    # Шаг 1 — находим начало блока данных
    start_idx = _find_data_start(lines)
    if start_idx == -1:
        return []

    # Шаг 2 — парсим только строки с данными
    result = []
    for line in lines[start_idx:]:
        if not line.strip() or re.match(r'^-{3,}$', line.strip()):
            continue

        entry = _parse_data_line(line)
        if entry:
            result.append(entry)

    return result


def _find_data_start(lines: List[str]) -> int:
    """Ищет индекс первой строки с данными (после заголовка и разделителя)"""
    for i, line in enumerate(lines):
        lower = line.lower()
        # Признаки заголовка
        if any(word in lower for word in ['local intf', 'local interface', 'neighbor dev', 'neighbor device']):
            # Следующая строка после заголовка и возможного разделителя
            for j in range(i + 1, min(i + 6, len(lines))):
                if re.match(r'^-{3,}$', lines[j].strip()) or not lines[j].strip():
                    continue
                if _looks_like_data_line(lines[j]):
                    return j
            # Если разделителя нет — берём следующую строку после заголовка
            return i + 1
    # Если заголовок не найден — считаем, что данные начинаются сразу
    return 0 if _looks_like_data_line(lines[0]) else -1


def _looks_like_data_line(line: str) -> bool:
    """Простая эвристика: строка начинается с интерфейса и содержит цифры в конце или hostname"""
    stripped = line.strip()
    if not stripped:
        return False
    # Начинается с интерфейса (GE, XGE, 10GE, Eth-Trunk и т.д.)
    if re.match(r'^[A-Za-z0-9/-]+[0-9/]+', stripped):
        # И содержит хотя бы одну цифру (exptime или в имени)
        if any(c.isdigit() for c in stripped):
            return True
    return False


def _parse_data_line(line: str) -> Optional[Dict[str, str]]:
    """
    Пытается разобрать одну строку данных несколькими способами (по убыванию надёжности)
    """
    # Способ 1: четыре столбца (самый частый) — local dev neigh exptime
    m = re.match(
        r'^(\S+?)\s{2,}'  # local_intf
        r'(.+?)\s{1,}'  # neighbor_dev (жадно до следующего большого пробела)
        r'(\S+?)\s{2,}'  # neighbor_intf
        r'(\d+)\s*$',  # exptime
        line
    )
    if m:
        local, dev, neigh, exp = m.groups()
        return _make_entry(local, dev, neigh, exp)

    # Способ 2: local exptime neigh dev
    m = re.match(
        r'^(\S+?)\s{2,}'  # local
        r'(\d+)\s{2,}'  # exptime
        r'(\S+?)\s{2,}'  # neigh intf
        r'(.+?)\s*$',  # dev (всё остальное)
        line
    )
    if m:
        local, exp, neigh, dev = m.groups()
        return _make_entry(local, dev, neigh, exp)

    # Способ 3: разбиение по двойным пробелам (fallback)
    parts = re.split(r'\s{2,}', line.strip())
    cleaned = [p.strip() for p in parts if p.strip()]

    if len(cleaned) < 3:
        return None

    # Ищем exptime — обычно 2–3 цифры
    exp_candidates = [p for p in cleaned if p.isdigit() and 1 <= len(p) <= 3]

    if exp_candidates:
        exp = exp_candidates[-1]  # берём последний как наиболее вероятный
        idx = cleaned.index(exp)

        if idx == 1:  # local | exp | intf | dev...
            local = cleaned[0]
            neigh = cleaned[2]
            dev = ' '.join(cleaned[3:]) if len(cleaned) > 3 else cleaned[2]
        else:  # local | dev... | intf | exp
            local = cleaned[0]
            neigh = cleaned[-2] if len(cleaned) > 3 else cleaned[-1]
            dev = ' '.join(cleaned[1:idx])
    else:
        # Нет явного exptime — считаем классический порядок
        local = cleaned[0]
        neigh = cleaned[-2]
        dev = ' '.join(cleaned[1:-1])
        exp = ''

    return _make_entry(local, dev, neigh, exp)


def _make_entry(
        local: str,
        dev: str,
        neigh: str,
        exp: str = ''
) -> Dict[str, str]:
    """Формирует итоговый словарь, чистит значения"""
    return {
        'local_intf': local.strip(),
        'neighbor_dev': re.sub(r'\s+', ' ', dev.strip()),
        'neighbor_intf': neigh.strip(),
        'exptime': exp.strip(),
    }


# ─── Сбор и отрисовка топологии (без изменений) ─────────────────────────────────
DEFAULT_STYLE = {
    "graph": {
        "splines": "ortho",  # true / polyline / curved / ortho
        "overlap": "false",
        "nodesep": "0.6",
        "ranksep": "0.9",
        "rankdir": "LR",  #TB - top / LR слева- направо
        "fontname": "Arial",
        "fontsize": "12",
        "dpi": "96",
        # "edge": {"dir": "none"},
        # "fontname": "Helvetica,Arial,sans-serif",
        # "fontsize": "14",
        # "rankdir": "LR",
        # "splines": "true",
        # "bgcolor": "transparent",
        # "center": "1",
        # "pad": "0.5",
        # "edge": {"dir": "none"},  # без стрелок
    },
    "node_default": {
        "shape": "box",
        "style": "rounded,filled",
        "fillcolor": "aliceblue",
        "fontname": "Helvetica,Arial,sans-serif",
        "fontsize": "11",
        "penwidth": "1.2",
    },
    "edge_default": {
        "fontname": "Helvetica,Arial,sans-serif",
        "fontsize": "10",
        "arrowsize": "0.9",
        "color": "darkblue",
        "fontcolor": "darkblue",
        "penwidth": "1.2",
    },
    "edge_label_prefix": "",  # можно поставить "via " или ""
    "device_name_transform": lambda x: x.strip(),  # как обрабатывать имя соседа
}


def collect_and_draw_topology(
        nr,
        output_file: str = "huawei_lldp_topology",
        style_config: Optional[Dict] = None,
        save_outputs: bool = False,
        use_saved: bool = False,
        output_dir: str = "device_outputs",
) -> None:
    """
    Собирает топологию из LLDP и рисует в Graphviz с настраиваемым стилем.

    style_config может содержать ключи:
      - graph, node_default, edge_default — словари атрибутов
      - edge_label_prefix — префикс для метки ребра
      - device_name_transform — функция преобразования имени устройства
    """
    style = DEFAULT_STYLE.copy()
    if style_config:
        for k in ["graph", "node_default", "edge_default"]:
            if k in style_config:
                style[k].update(style_config[k])
        # обновляем не-словари напрямую
        for k in ["edge_label_prefix", "device_name_transform"]:
            if k in style_config:
                style[k] = style_config[k]

    dot = Digraph(
        comment='LLDP Topology - Huawei',
        format='png',
        graph_attr=style["graph"],
        node_attr=style["node_default"],
        edge_attr=style["edge_default"],
    )

    topology: Dict[str, List[Tuple[str, str, str]]] = {}
    seen_edges = set()

    ## Блок сохранения вывода###########################################
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

        name_transform = style["device_name_transform"]

        for n in neighbors:
            neigh_raw = name_transform(n["neighbor_dev"])
            local_p = n["local_intf"]
            remote_p = n["neighbor_intf"]

            neigh = neigh_raw.rstrip('.')

            topology[host].append((neigh, local_p, remote_p))

            edge_key = tuple(sorted([host, neigh]) + [local_p, remote_p])  # чтобы избежать дубликатов в обе стороны
            if edge_key in seen_edges:
                continue
            seen_edges.add(edge_key)

            label = f"{style['edge_label_prefix']}{local_p} → {remote_p}"
            dot.edge(host, neigh, label=label)

    if not seen_edges:
        print("Связей не найдено")
        return

    # 1. Явно сохраняем .dot-файл
    dot.save(filename=f"{output_file}.dot")

    # 2. Рендерим в PNG
    # dot.render(output_file, view=True, cleanup=True)
    # print(f"\nСхема сохранена: {output_file}.png")

    # вывод текстового списка связей (опционально можно вынести или отключить)
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
        logging={"enabled": False}  # или True для отладки
    )

    print("=== Сбор LLDP-топологии с Huawei (Nornir + Netmiko) ===\n")
    collect_and_draw_topology(
        nr,
        save_outputs=True,  # Сохранить выводы в файлы
        use_saved=False,  # Использовать сохраненные файлы (если True, не подключается к устройствам)
        output_dir="device_outputs"
    )


if __name__ == "__main__":
    main()
