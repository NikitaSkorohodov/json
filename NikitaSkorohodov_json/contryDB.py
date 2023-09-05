import sqlite3
import json

# Устанавливаем соединение с базой данных (замените 'database.db' на путь к вашей базе данных)
conn = sqlite3.connect('world.db')
cursor = conn.cursor()

# Выполняем SQL-запрос для выбора данных о странах (замените 'countries' и 'population' на соответствующие названия таблицы и столбца в вашей базе данных)
cursor.execute("SELECT Name FROM country")

# Извлекаем данные из результата запроса
data = cursor.fetchall()

# Закрываем соединение с базой данных
conn.close()

# Создаем список словарей с данными о странах
countries_data = []
for row in data:
    Name = row
    countries_data.append({
        'Name': Name
    })

# Сохраняем данные в JSON файл (замените 'countries.json' на имя файла, который вы хотите создать)
with open('countries.json', 'w', encoding='utf-8') as json_file:
    json.dump(countries_data, json_file, ensure_ascii=False, indent=4)

print("Данные о странах сохранены в файл 'countries.json'")
