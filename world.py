import json
import mysql.connector
from decimal import Decimal

# Подключение к базе данных
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    database="world"
)

# Создание курсора для выполнения SQL-запросов
cursor = db_connection.cursor()

# Выполнение SQL-запроса для извлечения данных о странах
query = "SELECT * FROM Country"
cursor.execute(query)

# Извлечение результатов запроса
countries = []
for row in cursor.fetchall():
    country = {
        "Code": row[0],
        "Name": row[1],
        "Continent": row[2],
        "Region": row[3],
        "Cities": [],  # Создаем пустой список для городов
        "Languages": []  # Создаем пустой список для языков
    }

    # Выполнение SQL-запроса для извлечения городов для данной страны
    city_query = f"SELECT * FROM City WHERE CountryCode = '{country['Code']}'"
    cursor.execute(city_query)
    
    # Извлечение и добавление информации о городах
    for city_row in cursor.fetchall():
        city = {
            "Name": city_row[1],
            "District": city_row[3],
            "Population": float(city_row[4])  # Преобразование Decimal в float
        }
        country["Cities"].append(city)

    # Выполнение SQL-запроса для извлечения языков для данной страны
    language_query = f"SELECT * FROM CountryLanguage WHERE CountryCode = '{country['Code']}'"
    cursor.execute(language_query)
    
    # Извлечение и добавление информации о языках
    for language_row in cursor.fetchall():
        language = {
            "Language": language_row[1],
            "IsOfficial": language_row[2],
            "Percentage": float(language_row[3])  # Преобразование Decimal в float
        }
        country["Languages"].append(language)

    countries.append(country)

# Закрываем курсор и соединение с базой данных
cursor.close()
db_connection.close()

# Сохранение данных в JSON файл
with open('countries_with_cities_and_languages.json', 'w', encoding='utf-8') as json_file:
    json.dump(countries, json_file, ensure_ascii=False, indent=4)

print("Данные успешно сохранены в countries_with_cities_and_languages.json")
