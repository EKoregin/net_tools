# -*- coding: utf-8 -*-
"""
Скрипт поиска MAC-адреса на оборудовании сети.
Данные об оборудовании берет из Netbox
NOTE: Работает с Huawei и Juniper

Формат файла .env
NETBOX_URL = "https://netbox.domain.com"
TOKEN = "e998dklsdf987fsljdsf99798lsdf979j"
USER = "admin"
PASSWORD = "password"

Запуск:
python.exe .\find_mac_in_network.py --mac=b4:2d:56:8c:ee:6e --tenant=sc-berlin
"""
import argparse
import yaml
import os
from pathlib import Path
from nornir import InitNornir
from nornir.core.plugins.inventory import InventoryPluginRegister
from nornir.plugins.inventory.simple import SimpleInventory
from nornir_netmiko.tasks import netmiko_send_command
import pynetbox
from dotenv import load_dotenv

load_dotenv()
NETBOX_URL = os.getenv("NETBOX_URL")
TOKEN = os.getenv("TOKEN")
COMMAND = "display mac-address"
USERNAME = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
TENANT = os.getenv("TENANT").lower()
JUNIPER = "juniper"

if not all([NETBOX_URL, TOKEN, COMMAND, USERNAME, PASSWORD]):
    raise ValueError("Не заданы обязательные переменные")

InventoryPluginRegister.register("SimpleInventory", SimpleInventory)


# Получение хостов из Netbox
def load_devices_from_netbox(tenant: str = TENANT):
    print(f"Поиск устройств для {tenant} в Netbox")
    nb = pynetbox.api(NETBOX_URL, TOKEN)
    devices = nb.dcim.devices.filter(
        role=['aggregation', 'access', 'edge'],
        tenant=tenant,
        manufacturer=['huawei','juniper']
    )
    if not devices:
        print("Устройства не найдены по заданным критериям")
        exit()

    data = []
    for dev in devices:
        ip_addr = dev.primary_ip.address
        print(dev.name, ip_addr)
        dev_data = {
            'name': dev.name,
            'host': ip_addr.split('/', 1)[0],
            'username': USERNAME,
            'password': PASSWORD,
            'vendor': dev.device_type.manufacturer.name.lower(),
        }
        data.append(dev_data)

    return data


def create_temp_hosts_yaml(devices: list[dict], temp_file: str = "hosts_temp.yaml") -> Path:
    """
    Создаёт временный файл hosts_temp.yaml в формате, понятном SimpleInventory
    """
    data = {}

    for dev in devices:
        name = dev["name"]
        data[name] = {
            "hostname": dev["host"],  # ← IP или FQDN
            "platform": dev["vendor"],
            "username": dev.get("username", "admin"),
            "password": dev.get("password", ""),
            "connection_options": {
                "netmiko": {
                    "extras": {
                        "read_timeout_override": 20,
                        "device_type": dev["vendor"],
                        "conn_timeout": 20,
                        "global_delay_factor": 0.5,
                        "fast_cli": False,
                    }
                }
            }
        }

    path = Path(temp_file)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")

    return path


def load_nornir_with_temp_file(devices: list[dict]) -> InitNornir:
    """
    1. Создаёт временный yaml
    2. Проверяет его существование и размер
    3. Инициализирует Nornir
    """
    temp_path = create_temp_hosts_yaml(devices)

    # Проверка файла
    if not temp_path.is_file():
        raise FileNotFoundError(f"Не удалось создать файл: {temp_path}")

    if temp_path.stat().st_size == 0:
        raise ValueError(f"Созданный файл пустой: {temp_path}")

    print(f"Временный инвентарь создан: {temp_path} ({temp_path.stat().st_size} байт)")

    nr = InitNornir(
        inventory={
            "plugin": "SimpleInventory",
            "options": {
                "host_file": str(temp_path),
            }
        },
        logging={"enabled": False},
    )

    print(f"Загружено хостов: {len(nr.inventory.hosts)}")
    return nr


def normalize_mac(mac: str, device_type: str = "huawei") -> str:
    cleaned = (
        mac.lower()
        .replace(":", "")
        .replace("-", "")
        .replace(".", "")
        .replace(" ", "")
    )

    if len(cleaned) != 12 or not all(c in "0123456789abcdef" for c in cleaned):
        raise ValueError(f"Некорректный MAC-адрес: '{mac}'")

    if device_type.lower() in ("huawei", "huawey"):
        # xxxx-xxxx-xxxx
        return f"{cleaned[0:4]}-{cleaned[4:8]}-{cleaned[8:12]}"

    elif device_type.lower() == "juniper":
        # xx:xx:xx:xx:xx:xx
        return ":".join(cleaned[i:i + 2] for i in range(0, 12, 2))

    else:
        raise ValueError(f"Неизвестный тип устройства: '{device_type}'. "
                         "Ожидается 'huawei' или 'juniper'")


def find_mac_in_network(nr, mac: str) -> None:

    print(f"\nПоиск MAC-адреса: {mac}\n")

    def find_mac_task(task):
        hostname = task.host.name
        ip = task.host.hostname
        vendor = task.host.platform
        task.host["output"] = []
        try:
            # Формируем команду с нормализованным MAC
            mac_normalized = normalize_mac(mac, vendor)
            cmd = f"display mac-address {mac_normalized}"

            if vendor.lower() == JUNIPER:
                cmd = f"show ethernet-switching table {mac_normalized}"

            result = task.run(
                task=netmiko_send_command,
                command_string=cmd,
                severity_level=10,  # не считать отсутствие MAC ошибкой
            )

            output = result.result.strip()

            # Проверяем наличие MAC в выводе (в нижнем регистре)
            result = check_mac_in_output(hostname, ip, mac_normalized, output)
            task.host["output"].append(result)

        except Exception as e:
            print(f"Ошибка на {hostname} ({ip}): {e}")

    def check_mac_in_output(hostname, ip, mac_normalized, output):
        result = ""
        if mac_normalized in output.lower():
            lines = output.splitlines()
            found_lines = []

            for line in lines:
                line_clean = line.strip()
                if mac_normalized in line_clean.lower() and line_clean:
                    found_lines.append(line_clean)

            if found_lines:
                lines = []
                lines.append(f"\nНайден MAC {mac_normalized} на устройстве: {hostname} ({ip})")
                lines.append('-' * 100)
                lines.extend(found_lines)
                if not any("ae" in line for line in found_lines) and not any("Trunk" in line for line in found_lines):
                    lines.append("!!!!!!!!!!!!!!!!!!! ХОСТ ПОДКЛЮЧЕН СЮДА !!!!!!!!!!!!!!!!!!!!!")
                lines.append('=' * 100)
                result = "\n".join(lines)
                return result
        else:
            print(f"На {hostname} не найден")
            return f"На {hostname} не найден"


    # Запускаем задачу на всех хостах
    nr.run(task=find_mac_task)
    for host in nr.inventory.hosts.values():
        for task in host["output"]:
            if task:
                print(task)
    print(f"\nПоиск завершён.")


def main():
    parser = argparse.ArgumentParser(description="Скрипт для поиска MAC-адреса на устройствах сети")
    parser.add_argument("--mac", required=True, help="MAC-адрес для поиска")
    parser.add_argument("--tenant", required=True, help="Имя площадки")
    args = parser.parse_args()
    devices = load_devices_from_netbox(args.tenant)
    if not devices:
        print("Нет устройств из NetBox → выход")
        return

    try:
        nr = load_nornir_with_temp_file(devices)
        print(f"=== Поиск MAC-адреса в {args.tenant} ===\n")
        find_mac_in_network(nr, args.mac)
    except Exception as e:
        print(f"Ошибка при инициализации Nornir:\n{e}")


if __name__ == "__main__":
    main()
