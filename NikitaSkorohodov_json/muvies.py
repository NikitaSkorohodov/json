import json

# Открываем файл с данными о фильмах
with open('movies.json', 'r', encoding='utf-8') as file:
    movies = json.load(file)

# Создаем словарь для группировки фильмов по жанрам
grouped_movies = {}

# Группируем фильмы по жанрам
for movie in movies:
    genres = movie['Genre']
    for genre in genres:
        if genre not in grouped_movies:
            grouped_movies[genre] = []
        grouped_movies[genre].append(movie)

# Сортируем жанры по алфавиту
sorted_genres = sorted(grouped_movies.keys())

# Создаем и записываем HTML-файл
with open('movies.html', 'w', encoding='utf-8') as html_file:
    html_file.write("<html>\n")
    html_file.write("<head><title>Фильмы по жанрам</title></head>\n")
    html_file.write("<body>\n")
    
    for genre in sorted_genres:
        html_file.write(f'<h2>{genre}</h2>\n')
        for movie in grouped_movies[genre]:
            html_file.write(f'<p>Название: {movie["Title"]}</p>\n')
            html_file.write(f'<p>Год: {movie["Year"]}</p>\n')
            html_file.write(f'<p>Рейтинг: {movie["Rated"]}</p>\n')
            html_file.write(f'<p>Режиссер: {movie["Director"]}</p>\n')
            html_file.write(f'<p>Актеры: {", ".join(movie["Actors"])}</p>\n')
            html_file.write(f'<p>Сюжет: {movie["Plot"]}</p>\n')
            html_file.write("<hr>\n")
    
    html_file.write("</body>\n")
    html_file.write("</html>\n")
