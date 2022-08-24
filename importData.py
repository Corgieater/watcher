from dotenv import load_dotenv
from mysql.connector import pooling
import os
from datetime import datetime
import csv

load_dotenv()
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = 'movie_web'
FOLDER_LOCATION = os.getenv('FOLDER_LOCATION')

today = datetime.today()
date = today.strftime("%Y-%m-%d")
file_name = f'movie_{date}'

p = pooling.MySQLConnectionPool(
            pool_name='pool',
            pool_size=5,
            pool_reset_session=True,
            host=MYSQL_HOST,
            database=MYSQL_DATABASE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD)


class MovieDatabase:
    # movie添加
    def check_movie_exist(self, title, year):
        connection = p.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute('SELECT movie_id FROM movies_info \n'
                           'WHERE title = %s\n'
                           'AND year = %s', (title, year))
            movie_exist = cursor.fetchall()
            if movie_exist:
                movie_exist = True
            else:
                movie_exist = False
        except Exception as e:
            print(e)
        else:
            return movie_exist
        finally:
            cursor.close()
            connection.close()

    def add_movie_to_database(self, title, year, story_line, tagline):
        connection = p.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute('INSERT INTO movies_info(movie_id, title, year, story_line, tagline) '
                           'VALUES(DEFAULT,%s,%s,%s,%s)', (title, year, story_line, tagline))
            cursor.execute('SELECT LAST_INSERT_ID()')
            last_insert_movie_id = cursor.fetchone()[0]
        except Exception as e:
            with open(f'{FOLDER_LOCATION}{file_name}_movie_adding_error.csv', 'w', newline='', encoding='utf8') \
                    as f:
                writer = csv.writer(f)
                writer.writerow([str(e), title])
            connection.rollback()
            return False
        else:
            connection.commit()
            return last_insert_movie_id
        finally:
            cursor.close()
            connection.close()

    # GENRE添加
    def find_or_add_genre_to_database(self, genre):
        connection = p.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute('SELECT genre_id FROM genres WHERE type = %s,', (genre,))
            genre_id = cursor.fetchone()
            if genre_id is not None:
                genre_id = genre_id[0]
            else:
                cursor.execute('INSERT INTO genres(genre_id, type) '
                               'VALUES(DEFAULT,%s)', (genre,))
                cursor.execute('SELECT LAST_INSERT_ID()')
                genre_id = cursor.fetchone()[0]
        except Exception as e:
            print(e)
            with open(f'{FOLDER_LOCATION}{file_name}_genre_adding_error.csv', 'w', newline='', encoding='utf8') \
                    as f:
                writer = csv.writer(f)
                writer.writerow([str(e), genre])
            connection.rollback()
            return False
        else:
            connection.commit()
            return genre_id
        finally:
            cursor.close()
            connection.close()

    # 找或添加導演
    def find_or_add_director_to_database(self, director):
        connection = p.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute('SELECT director_id FROM directors WHERE name = %s', (director,))
            director_id = cursor.fetchone()
            if director_id is not None:
                director_id = director_id[0]
            else:
                cursor.execute('INSERT INTO directors(director_id, name) '
                               'VALUES(DEFAULT,%s)', (director,))
                cursor.execute('SELECT LAST_INSERT_ID()')
                director_id = cursor.fetchone()[0]

        except Exception as e:
            print(e)
            with open(f'{FOLDER_LOCATION}{file_name}_director_adding_error.csv', 'w', newline='', encoding='utf8') \
                    as f:
                writer = csv.writer(f)
                writer.writerow([str(e), director])
            connection.rollback()
            return False
        else:
            connection.commit()
            return director_id
        finally:
            cursor.close()
            connection.close()

    # 找或添加演員
    def find_or_add_actor_to_database(self, actor):
        connection = p.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute('SELECT actor_id, name FROM actors WHERE name = %s', (actor,))
            actor_id = cursor.fetchone()
            if actor_id is not None:
                actor_id = actor_id[0]
            else:
                cursor.execute('INSERT INTO actors(actor_id, name) '
                               'VALUES(DEFAULT,%s)', (actor,))
                cursor.execute('SELECT LAST_INSERT_ID()')
                actor_id = cursor.fetchone()[0]
        except Exception as e:
            print(e)
            with open(f'{FOLDER_LOCATION}{file_name}_actor_adding_error.csv', 'w', newline='', encoding='utf8') \
                    as f:
                writer = csv.writer(f)
                writer.writerow([str(e), actor])
            connection.rollback()
            return False
        else:
            connection.commit()
            return actor_id
        finally:
            cursor.close()
            connection.close()

    # 添加導演電影關係鏈進關係table
    def add_directors_movies_relationship(self, director_id, movie_id):
        connection = p.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute('INSERT INTO directors_movies(dm_id, dm_director_id, dm_movie_id) '
                           'VALUES(DEFAULT, %s, %s)', (director_id, movie_id))
        except Exception as e:
            print(e)
            with open(f'{FOLDER_LOCATION}{file_name}_directorMovies_adding_error.csv', 'w',
                      newline='', encoding='utf8') \
                    as f:
                writer = csv.writer(f)
                writer.writerow([str(e), director_id, movie_id])
            connection.rollback()
            return False
        else:
            connection.commit()
            return True
        finally:
            cursor.close()
            connection.close()

    # 添加GENRE電影關係鏈進關係table
    def add_genre_movies_relationship(self, genre_id, movie_id):
        connection = p.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute('INSERT INTO genres_movies(gm_id, gm_genre_id, gm_movie_id) '
                           'VALUES(DEFAULT, %s, %s)', (genre_id, movie_id))
        except Exception as e:
            print(e)
            with open(f'{FOLDER_LOCATION}{file_name}_genreMovies_adding_error.csv', 'w',
                      newline='', encoding='utf8') \
                    as f:
                writer = csv.writer(f)
                writer.writerow([str(e), genre_id, movie_id])
            connection.rollback()
            return False
        else:
            connection.commit()
            return True
        finally:
            cursor.close()
            connection.close()

    # 添加演員電影關係鏈進關係table
    def add_actor_movies_relationship(self, actor_id, movie_id):
        connection = p.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute('INSERT INTO actors_movies(am_id, am_actor_id, am_movie_id) '
                           'VALUES(DEFAULT, %s, %s)', (actor_id, movie_id))
        except Exception as e:
            print(e)
            with open(f'{FOLDER_LOCATION}{file_name}_actorMovies_adding_error.csv', 'w',
                      newline='', encoding='utf8') \
                    as f:
                writer = csv.writer(f)
                writer.writerow([str(e), actor_id, movie_id])
            connection.rollback()
            return False
        else:
            connection.commit()
            return True
        finally:
            cursor.close()
            connection.close()


