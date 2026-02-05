import re
from datetime import datetime
from pathlib import Path
import pandas as pd
import os
import argparse
from netmiko import ConnectHandler
from netmiko.exceptions import NetmikoTimeoutException, NetmikoAuthenticationException
import pynetbox
from dotenv import load_dotenv

"""
Работает только с Huawei
Подключение к хостам и выполнение команды "display ip routing-table protocol direct"
Сохранение вывода в файлы.
Парсинг файлов и создание сборной таблицы с данными:
Network, Protocol, Device, Interface, NextHop

По умолчанию данные о хостах берутся из файла devices.yaml
Флаги
--netbox // Данные будут браться из Netbox API
--collect //Версия с промежуточным сохранением в файлы (по умолчанию)
--from-files //Повторная обработка без подключения (после первого запуска)

Примеры:
Данные о хостах из Netbox с сохранением в файлы
python.exe .\direct_routes_for_net.py --netbox --collect

Повторная обработка существующих файлов (без подключения)
python.exe .\direct_routes_for_net.py --from-files

Данные о хостах из файл devices.yaml, с сохранение в файлы
python.exe .\direct_routes_for_net.py


Конфигурационные данные берутся из файлов devices.yaml и .env
Если используется флаг --netbox, то devices.yaml не нужен.  
Формат файла devices.yaml
# devices.yaml
- host: 10.10.0.10
- host: 10.10.0.11

Формат файла .env
NETBOX_URL = "https://netbox.domain.com"
TOKEN = "e998dklsdf987fsljdsf99798lsdf979j"
COMMAND = 'display ip routing-table protocol direct'
USER = "admin"
PASSWORD = "password"
RESULT_CSV = "routing_table_summary.csv"
RAW_OUTPUT_DIR = "direct_routes_raw_outputs"
TENANT = "berlin"
"""
# ------------------------------------------------------------------------------
# Настройки
# ------------------------------------------------------------------------------
load_dotenv()
NETBOX_URL = os.getenv("NETBOX_URL")
TOKEN = os.getenv("TOKEN")
COMMAND = os.getenv("COMMAND")
USERNAME = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
RESULT_CSV = os.getenv("RESULT_CSV")
RAW_OUTPUT_DIR = os.getenv("RAW_OUTPUT_DIR")
OUT_DIR     = "direct_routes"
# Название арендатора, если хосты берутся из Netbox
TENANT = os.getenv("TENANT").lower()

if not all([NETBOX_URL, TOKEN, COMMAND, USERNAME, PASSWORD, RESULT_CSV, RAW_OUTPUT_DIR]):
    raise ValueError("Не заданы обязательные переменные")

os.makedirs(RAW_OUTPUT_DIR, exist_ok=True)

PARSE_PATTERN = re.compile(
    r'^\s*'
    r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2})\s+'
    r'Direct\s+\d+\s+\d+\s+\w+\s+'
    r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+'
    r'(\S+)',
    re.MULTILINE
)

# ------------------------------------------------------------------------------
# Аргументы командной строки
# ------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description="Сбор direct-маршрутов с Huawei")
parser.add_argument('--collect', action='store_true',
                    help="Собрать данные с устройств (по умолчанию)")
parser.add_argument('--from-files', action='store_true',
                    help="Обработать уже сохранённые сырые файлы вместо подключения")
parser.add_argument('--devices-file', type=str, default="devices.yaml",
                    help="Путь к файлу со списком устройств (yaml или json)")
parser.add_argument('--netbox', action='store_true',
                    help="Брать данные о хостах из Netbox")
args = parser.parse_args()


# ------------------------------------------------------------------------------
# Функции
# ------------------------------------------------------------------------------

# Получение хостов из файла devices.yaml
def load_devices_from_file(file_path):
    """Попытка загрузить список устройств из yaml"""
    import yaml
    try:
        with open(file_path, encoding='utf-8') as f:
            data = yaml.safe_load(f)

        if isinstance(data, list):
            for host in data:
                host.update({
                    'username': USERNAME,
                    'password': PASSWORD,
                })
            return data
        print("Неверный формат файла устройств")
        return []
    except Exception as e:
        print(f"Ошибка чтения файла устройств: {e}")
        return []


# Получение хостов из API netbox
def load_devices_from_netbox():
    nb = pynetbox.api(NETBOX_URL, TOKEN)
    devices = nb.dcim.devices.filter(
        role=['aggregation'],
        tenant=TENANT,
        manufacturer="huawei"
    )
    if not devices:
        print("Устройства не найдены по заданным критериям")
        exit()

    data = []
    for dev in devices:
        ip_addr = dev.primary_ip.address
        print(dev.name, ip_addr)
        dev_data = {
            'host': ip_addr.replace("/32", ""),
            'username': USERNAME,
            'password': PASSWORD
        }
        data.append(dev_data)

    return data


def get_raw_filename(host):
    """Имя файла для сохранения сырого вывода"""
    safe_host = host.replace('.', '_').replace(':', '_')
    return os.path.join(RAW_OUTPUT_DIR, f"{safe_host}.txt")


def collect_from_device(device):
    """Подключение → выполнение команды → сохранение сырого вывода"""
    host = device['host']
    filename = get_raw_filename(host)

    try:
        conn = ConnectHandler(
            host=host,
            username=device.get('username'),
            password=device.get('password'),
            device_type='huawei',
            global_delay_factor=2,
            conn_timeout=15,
        )

        hostname = conn.find_prompt().strip('<> #')
        print(f"Подключено: {host} → {hostname}")

        output = conn.send_command(COMMAND, strip_prompt=False, strip_command=False)
        conn.disconnect()

        # Сохраняем сырой вывод
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"--- {hostname} ({host}) ---\n")
            f.write(output)
            f.write("\n")

        print(f"Сохранён вывод: {filename}")
        return hostname, output

    except (NetmikoTimeoutException, NetmikoAuthenticationException) as e:
        print(f"Ошибка подключения {host}: {e}")
        return None, None
    except Exception as e:
        print(f"Неизвестная ошибка {host}: {e}")
        return None, None


def parse_output(hostname, output):
    """Парсинг одного вывода команды"""
    if not output:
        return []

    matches = PARSE_PATTERN.findall(output)
    rows = []

    for net, nexthop, iface in matches:
        if '127.0.0.' in net or '127.0.0.' in nexthop:
            continue
        rows.append({
            'Network': net,
            'Protocol': 'Direct',
            'Device': hostname,
            'Interface': iface,
            'NextHop': nexthop
        })

    return rows


def process_raw_files():
    """Чтение и парсинг всех сохранённых файлов"""
    all_rows = []

    for filename in os.listdir(RAW_OUTPUT_DIR):
        if not filename.endswith('.txt'):
            continue
        path = os.path.join(RAW_OUTPUT_DIR, filename)

        with open(path, encoding='utf-8') as f:
            content = f.read()

        # Пытаемся извлечь hostname из первой строки
        first_line = content.split('\n', 1)[0].strip()
        if first_line.startswith('---') and '---' in first_line:
            hostname_part = first_line.strip('--- ').split(' (')[0]
        else:
            hostname_part = filename.replace('.txt', '').replace('_', '.')

        rows = parse_output(hostname_part, content)
        all_rows.extend(rows)

    return all_rows


def save_results(df: pd.DataFrame, base_name: str = "routes"):
    """Сохраняет DataFrame в CSV и Excel"""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

    csv_path = os.path.join(OUT_DIR, f"{base_name}_{timestamp}.csv")
    xlsx_path = os.path.join(OUT_DIR, f"{base_name}_{TENANT}_{timestamp}.xlsx")

    try:
        # CSV
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"Сохранено в CSV:  {csv_path}")

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

        print(f"Сохранено в Excel: {xlsx_path}")

    except Exception as e:
        print(f"Ошибка при сохранении файлов: {e}")


# ------------------------------------------------------------------------------
# Основная логика
# ------------------------------------------------------------------------------
def main():
    data = []

    if args.from_files:
        print("Режим: обработка сохранённых файлов")
        data = process_raw_files()

    else:
        if args.netbox:
            devices = load_devices_from_netbox()
        else:
            devices = load_devices_from_file(args.devices_file)
        if not devices:
            print("Не удалось загрузить список устройств")
            exit(1)

        print(f"Найдено устройств: {len(devices)}\n")

        for dev in devices:
            hostname, output = collect_from_device(dev)
            if output:
                rows = parse_output(hostname, output)
                data.extend(rows)

    if data:
        df = pd.DataFrame(data)
        df = df.sort_values(by=['Device', 'Network'])

        # Если хотите сохранить результат в файл
        save_results(df)

        # df.to_csv(RESULT_CSV, index=False, encoding='utf-8-sig')
        print(f"\nСохранено {len(df)} записей → {RESULT_CSV}")
    else:
        print("\nНе удалось собрать данные")

    print("Готово.")


if __name__ == "__main__":
    main()
