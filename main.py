from importData import *
import boto3
import requests
from os.path import basename
import random
import ast
import os.path
import time
from datetime import datetime
import sys

load_dotenv()

AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET = os.getenv('BUCKET')
FOLDER_LOCATION = os.getenv('FOLDER_LOCATION')

client = boto3.client('s3',
                      aws_access_key_id=AWS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY
                      )

database = MovieDatabase()


# for detecting file
today = datetime.today()
date = today.strftime("%Y-%m-%d")
file_name = f'movie_{date}'
ok_file = f'{file_name}_ok'


# 上傳Poster去S3
def upload_poster(poster_url, img_id):
    with open(basename(poster_url), 'wb') as f:
        client.put_object(
            Bucket=AWS_BUCKET,
            Body=requests.get(poster_url).content,
            Key=f'moviePos/img{img_id}.jpg',
            ContentType='image/jpeg'
        )


def random_delay(time_list):
    delay_choices = time_list
    delay = random.choice(delay_choices)
    time.sleep(delay)


def write_movies():
    # 開新檔案寫入
    with open(f'{FOLDER_LOCATION}{file_name}.csv', encoding='UTF-8') as movies:
        rows = csv.reader(movies)
        for row in rows:
            title = row[0]
            year = row[1]
            plot = row[2]
            tagline = row[3]
            genres = row[4]
            directors = row[5]
            try:
                directors = ast.literal_eval(directors)
            except Exception as e1:
                continue
            actors = row[6]
            try:
                actors = ast.literal_eval(actors)
            except Exception as e2:
                continue
            poster_url = row[7]

            try:
                genres = ast.literal_eval(genres)
            except Exception as e3:
                continue

            genres_num_list = []

            # 把已存在的genre換成mysql id
            for genre in genres:
                match genre:
                    case 'Drama':
                        genres_num_list.append(1)
                    case 'Crime':
                        genres_num_list.append(2)
                    case 'Thriller':
                        genres_num_list.append(3)
                    case 'Animation':
                        genres_num_list.append(4)
                    case 'Adventure':
                        genres_num_list.append(5)
                    case 'Family':
                        genres_num_list.append(6)
                    case 'Musical':
                        genres_num_list.append(7)
                    case 'Music':
                        genres_num_list.append(8)
                    case 'Action':
                        genres_num_list.append(9)
                    case 'Fantasy':
                        genres_num_list.append(10)
                    case 'Sci-Fi':
                        genres_num_list.append(11)
                    case 'Biography':
                        genres_num_list.append(12)
                    case 'Comedy':
                        genres_num_list.append(13)
                    case 'Romance':
                        genres_num_list.append(14)
                    case 'War':
                        genres_num_list.append(15)
                    case 'Horror':
                        genres_num_list.append(16)
                    case 'Western':
                        genres_num_list.append(17)
                    case 'Mystery':
                        genres_num_list.append(18)
                    case 'Film-Noir':
                        genres_num_list.append(19)
                    case 'History':
                        genres_num_list.append(20)
                    case 'Sport':
                        genres_num_list.append(21)
                    case 'Reality-Tv':
                        genres_num_list.append(22)
                    case 'Talk-Show':
                        genres_num_list.append(25)
                    case 'Game-Show':
                        genres_num_list.append(27)
                    case 'Adult':
                        genres_num_list.append(45)

                    case _:
                        genre_id = database.find_or_add_genre_to_database(genre)
                        genres_num_list.append(genre_id)
            try:
                movie_exist = database.check_movie_exist(title, year)
            except Exception as e4:
                with open(f'{FOLDER_LOCATION}{file_name}_exist_error.txt', 'w', newline='', encoding='utf8') \
                        as exist_f:
                    exist_f.write(str(e4))

            # 電影沒在資料庫就寫入
            if not movie_exist:
                movie_id = database.add_movie_to_database(title, year, plot, tagline)
                for genre_num in genres_num_list:
                    database.add_genre_movies_relationship(genre_num, movie_id)
                for director in directors:
                    director_id = database.find_or_add_director_to_database(director)
                    database.add_directors_movies_relationship(director_id, movie_id)
                for actor in actors:
                    actor_id = database.find_or_add_actor_to_database(actor)
                    database.add_actor_movies_relationship(actor_id, movie_id)
                upload_poster(poster_url, movie_id)
            else:
                continue


if __name__ == "__main__":
    # if there is a success_file means this process had already finished
    success_file = f'{FOLDER_LOCATION}{file_name}_success.txt'
    already_in_data = os.path.isfile(success_file)
    if already_in_data:
        sys.exit()
    else:
        all_files = os.listdir(FOLDER_LOCATION)

        for f in all_files:
            f = f'{FOLDER_LOCATION}{f}'
            current_file_name = os.path.basename(f)
            target_file = f'{FOLDER_LOCATION}{ok_file}.csv'
            if f.endswith('.csv'):
                file_second = os.path.getctime(f)
                file_date = datetime.fromtimestamp(file_second)
                now = datetime.now()
                # 檔案超過一週就刪除
                if (file_date - now).days > 7:
                    os.remove(f)
            # 刪掉沒用的圖片
            if f.endswith('.jpg'):
                os.remove(f)

            if f == target_file:
                try:
                    write_movies()
                except Exception as e:
                    with open(f'{FOLDER_LOCATION}{file_name}_error.txt', 'w', newline='', encoding='utf8') as error_f:
                        error_f.write(str(e))
                else:
                    with open(f'{FOLDER_LOCATION}{file_name}_success.txt', 'w', newline='', encoding='utf8') as success_f:
                        success_f.write('good')
