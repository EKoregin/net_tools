#!/usr/bin/env python3
"""
Скрипт парсит CSV файл с IP адресами и для каждого IP добавляет описание (и название VLAN если есть),
которое находит в Netbox.
Формат CSV-файла:
SrcAddr, DstAddr, Port, Protocol
Если поиск в Netbox находит несколько сетей, то выбирается самый длинный префикс.
Для Destination адрес ищется его Whois запись и вписывается домен.
Алгоритм обработки:
1. Убрать дубликаты при совпадении всех полей
2. Отсортировать по SrcAddr
3. Вывести результат

4. Создать множество префиксов сетей Prefixes.
5. Для каждого IP делать запрос в Prefixes.
Если в таблице нет подходящего префикса, то делать запрос в Netbox для поиска префикса.
Сохранять самый длинный найденный префикс в множество Prefixes.
6. Найденные префиксы добавлять в результат к каждой результирующей записи
"""

import pandas as pd

# ────────────────────────────────────────────────
# НАСТРОЙКИ — меняйте здесь
CSV_DIR = "csv"
INPUT_FILE = CSV_DIR + "/policy_org.csv"

# Список полей, которые нужно оставить
# Порядок в списке определяет порядок колонок в выводе
# Первое поле будет использовано для сортировки
NEEDED_COLUMNS = [
    "Source Address",
    "Destination Address",
    "Destination Port",
    "Protocol",
]

# ────────────────────────────────────────────────

def main():
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

        # Подсчитываем количество вхождений каждой уникальной комбинации
        counts = df.groupby(NEEDED_COLUMNS).size().reset_index(name='Count')

        # Сортируем по первому полю (или можно по Count descending, если нужно)
        sort_column = NEEDED_COLUMNS[0]
        counts = counts.sort_values(by=sort_column)

        # Выводим результат
        print("\nУникальные строки и количество их повторений:")
        print("-" * 80)

        # Красивый вывод с выравниванием
        header = "  ".join(f"{col:<18}" for col in NEEDED_COLUMNS) + "  Count"
        print(header)
        print("-" * 80)

        for _, row in counts.iterrows():
            line = "  ".join(f"{str(row[col]):<18}" for col in NEEDED_COLUMNS)
            print(f"{line}  {row['Count']}")

        print("-" * 80)
        print(f"Всего уникальных комбинаций: {len(counts)}")
        print(f"Всего строк в исходном файле: {len(df)}")

        # Если хотите сохранить результат в файл
        OUTPUT_FILE = CSV_DIR + "/unique_with_counts.csv"
        counts.to_csv(OUTPUT_FILE, index=False)
        print(f"\nСохранено в: {OUTPUT_FILE}")

    except FileNotFoundError:
        print(f"Файл не найден: {INPUT_FILE}")
    except Exception as e:
        print("Ошибка при обработке:")
        print(e)


if __name__ == "__main__":
    main()