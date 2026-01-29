import re
import pandas as pd
from netmiko import ConnectHandler
from netmiko.exceptions import NetmikoTimeoutException, NetmikoAuthenticationException

# Список устройств Huawei (L3-оборудование). Замените на свои IP/hostname, username, password.
# Пример: devices = [{'host': '192.168.1.1', 'username': 'admin', 'password': 'pass'}, ...]
devices = [
    # Добавьте здесь свои устройства, например:
    {'host': '10.181.96.50', 'username': 'koregin', 'password': 'AyrgeV5!=sF^E>P', 'device_type': 'huawei'},
    {'host': '10.181.96.51', 'username': 'koregin', 'password': 'AyrgeV5!=sF^E>P', 'device_type': 'huawei'},
]

# Команда для выполнения
command = 'display ip routing-table protocol direct'

# Регулярное выражение для парсинга строк таблицы. Захватываем: Network, NextHop, Interface
# Proto всегда 'Direct', Pre/Cost/Flags игнорируем для простоты, но проверяем наличие
parse_pattern = re.compile(
    r'^\s*(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2})\s+Direct\s+\d+\s+\d+\s+\w+\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+(\S+)',
    re.MULTILINE)

# Список для сбора данных
data = []

for device in devices:
    try:
        # Подключение к устройству
        net_connect = ConnectHandler(
            host=device['host'],
            username=device['username'],
            password=device['password'],
            device_type='huawei',  # Netmiko поддерживает Huawei
            global_delay_factor=2  # Для медленных устройств
        )

        # Получаем hostname устройства (из prompt или sysname)
        hostname = net_connect.find_prompt().strip('<>')  # Удаляем < >, получаем wh-penza-b1-ag-1

        # Выполняем команду
        output = net_connect.send_command(command, strip_prompt=False, strip_command=False)

        # Парсим вывод
        matches = parse_pattern.findall(output)

        for match in matches:
            network, nexthop, interface = match
            # Исключаем loopback 127.0.0.1 (если есть, хотя в direct обычно /8 или /32)
            if '127.0.0.1' in network or '127.0.0.0' in network or '127.0.0.1' in nexthop:
                continue
            data.append({
                'Network': network,
                'Protocol': 'Direct',
                'Device': hostname,
                'Interface': interface,
                'NextHop': nexthop
            })

        # Отключаемся
        net_connect.disconnect()

    except (NetmikoTimeoutException, NetmikoAuthenticationException) as e:
        print(f"Ошибка подключения к {device['host']}: {str(e)}")
        continue
    except Exception as e:
        print(f"Неизвестная ошибка на {device['host']}: {str(e)}")
        continue

# Если данные собраны, создаем DataFrame и сохраняем в CSV/Excel
if data:
    df = pd.DataFrame(data)
    # Сортируем по Network или Device (опционально)
    df = df.sort_values(by=['Device', 'Network'])

    # Сохраняем в CSV
    df.to_csv('routing_table_summary.csv', index=False)
    print("Данные сохранены в routing_table_summary.csv")

    # Или в Excel (если нужно, раскомментируйте)
    # df.to_excel('routing_table_summary.xlsx', index=False)
    # print("Данные сохранены в routing_table_summary.xlsx")
else:
    print("Нет данных для сохранения.")

# Пример использования: запустите скрипт с заполненным списком devices.