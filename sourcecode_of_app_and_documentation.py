from flask import Flask, jsonify, request
import mysql.connector
import jwt
import datetime
from flask_bcrypt import Bcrypt
from functools import wraps
from flasgger import Swagger

app = Flask(__name__)
db_initialized = False
app.config['SECRET_KEY'] = "cfe862e5b529c7b4db9ea101eb4ffba10cd9d37651dcd3fe8cb544ff9807e1b7"
bcrypt = Bcrypt(app)

swagger = Swagger(app, template={
    "info": {
        "title": "Film Recommendation System API",
        "description": "API for managing movies, recommendations, and user interactions.",
        "version": "1.0.0"
        },
    "securityDefinitions": {
        "BearerAuth": {
            "type": "apiKey",
            "name": "Authorization",
            "in": "header",
            "description": "Enter 'Bearer <your_token>'"
        }
    },
    "security": [{"BearerAuth": []}]
})


# Database connection configuration
# this configuration is automatically tries to connet with port 3306 please define it in config to not have conflict
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'film_recommendation_project'
}

def create_connection():
    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        raise
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')  # Expect 'Bearer <token>'
        if not token:
            return jsonify({'message': 'Token is missing!'}), 401
        try:
            token = token.split(" ")[1]  # Extract token after "Bearer"
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            current_user = data["user_id"]
        except Exception as e:
            return jsonify({'message': 'Token is invalid!', 'error': str(e)}), 401
        return f(current_user, *args, **kwargs)
    return decorated

@app.route('/')
def home():
    """
    Home Endpoint
    ---
    tags:
      - General
    responses:
      200:
        description: Welcome message
        content:
          application/json:
            schema:
              type: string
              example: Welcome to the Film Recommendation Project API!
    """
    return "Welcome to the Film Recommendation Project API!"


@app.route('/initialize-database', methods=['GET'])
def initialize_database():
    """
    Initialize the database and create tables
    ---
    tags:
      - Database
    responses:
      200:
        description: Database initialized successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Database and tables initialized successfully with relationships!
      403:
        description: Database already initialized
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Database has already been initialized!
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Error connecting to MySQL
    """
    global db_initialized
    if db_initialized:
        return jsonify({"message": "Database has already been initialized!"}), 403
    try:
        conn = create_connection()
        cursor = conn.cursor()

        # 1. user Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user (
            user_id INT AUTO_INCREMENT PRIMARY KEY,
            user_name VARCHAR(100) UNIQUE NOT NULL CHECK (LENGTH(user_name) > 3),
            email VARCHAR(100) UNIQUE NOT NULL CHECK (email LIKE '%_@_%.__%'),
            password VARCHAR(255) NOT NULL,
            preferences TEXT
        )ENGINE=INNODB;
        """)
        
        # Add a default admin user
        admin_password = "001"
        hashed_admin_password = bcrypt.generate_password_hash(admin_password).decode('utf-8')
        cursor.execute("""
        INSERT IGNORE INTO user (user_id, user_name, email, password, preferences)
        VALUES (1, 'admin', 'admin@example.com', %s, NULL);
        """, (hashed_admin_password,))
        
        # 2. movie Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS movie (
            movie_id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(200) NOT NULL,
            description TEXT,
            duration INT
        )ENGINE=INNODB;
        """)

        # 3. genre Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS genre (
            genre_id INT AUTO_INCREMENT PRIMARY KEY,
            genre_name VARCHAR(100) UNIQUE NOT NULL
        )ENGINE=INNODB;
        """)

        # 4. movie_genre Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS movie_genre (
            movie_id INT NOT NULL,
            genre_id INT NOT NULL,
            PRIMARY KEY (movie_id, genre_id),
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE,
            FOREIGN KEY (genre_id) REFERENCES genre(genre_id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE
        )ENGINE=INNODB;
        """)

        # 5. rating Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS rating (
            rating_id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            movie_id INT NOT NULL,
            score DECIMAL(2, 1) NOT NULL CHECK (score BETWEEN 1.0 AND 5.0),
            FOREIGN KEY (user_id) REFERENCES user(user_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE
        )ENGINE=INNODB;
        """)

        # 6. review Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS review (
            review_id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            movie_id INT NOT NULL,
            review_text TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES user(user_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE
        )ENGINE=INNODB;
        """)

        # 7. watch_history Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS watch_history (
            history_id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            movie_id INT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES user(user_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE
        )ENGINE=INNODB;
        """)

        # 8. recommendation Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS recommendation (
            recommendation_id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            movie_id INT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES user(user_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE
        )ENGINE=INNODB;
        """)

        conn.commit()
        cursor.close()
        conn.close()
        db_initialized = True
        return jsonify({"message": "Database and tables initialized successfully with relationships!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
#user table's endpoints

@app.route('/users', methods=['GET'])
@token_required
def get_all_users(current_user):
    """
    Get All Users
    ---
    tags:
      - User
    security:
      - BearerAuth: []
    responses:
      200:
        description: List of all users
        content:
          application/json:
            schema:
              type: array
              items:
                type: object
                properties:
                  user_id:
                    type: integer
                  user_name:
                    type: string
                  email:
                    type: string
                  preferences:
                    type: string
                    nullable: true
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "SELECT user_id, user_name, email, preferences FROM user"  # Exclude sensitive fields like password
        cursor.execute(query)
        users = cursor.fetchall()

        cursor.close()
        connection.close()
        return jsonify(users), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<int:user_id>', methods=['GET'])
@token_required
def get_user_by_id(current_user, user_id):
    """
    Get User by ID
    ---
    tags:
      - User
    security:
      - BearerAuth: []
    parameters:
      - name: user_id
        in: path
        required: true
        description: ID of the user
        schema:
          type: integer
    responses:
      200:
        description: User details
        content:
          application/json:
            schema:
              type: object
              properties:
                user_id:
                  type: integer
                user_name:
                  type: string
                email:
                  type: string
                preferences:
                  type: string
                  nullable: true
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      404:
        description: User not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: User not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != user_id:
        return jsonify({'message': 'Access denied'}), 403
    try:
        connection = create_connection()
        cursor = connection.cursor(dictionary=True)

        query = "SELECT user_id, user_name, email, preferences FROM user WHERE user_id = %s"
        cursor.execute(query, (user_id,))
        user = cursor.fetchone()

        cursor.close()
        connection.close()

        if user:
            return jsonify(user), 200
        else:
            return jsonify({'message': 'User not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<int:user_id>', methods=['PUT'])
@token_required
def update_user(current_user, user_id):
    """
    Update User
    ---
    tags:
      - User
    security:
      - BearerAuth: []
    parameters:
      - name: user_id
        in: path
        required: true
        description: ID of the user
        schema:
          type: integer
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              user_name:
                type: string
                example: new_username
              email:
                type: string
                example: new_email@example.com
              preferences:
                type: string
                example: action, comedy
    responses:
      200:
        description: User updated successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: User updated successfully
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      404:
        description: User not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: User not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != user_id:
        return jsonify({'message': 'Access denied'}), 403

    data = request.get_json()
    user_name = data.get('user_name')
    email = data.get('email')
    preferences = data.get('preferences')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "UPDATE user SET user_name = %s, email = %s, preferences = %s WHERE user_id = %s"
        cursor.execute(query, (user_name, email, preferences, user_id))

        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'User updated successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'User not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<int:user_id>', methods=['DELETE'])
@token_required
def delete_user(current_user, user_id):
    """
    Delete User
    ---
    tags:
      - User
    security:
      - BearerAuth: []
    parameters:
      - name: user_id
        in: path
        required: true
        description: ID of the user
        schema:
          type: integer
    responses:
      200:
        description: User deleted successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: User deleted successfully
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      404:
        description: User not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: User not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != user_id:
        return jsonify({'message': 'Access denied'}), 403
    if user_id == 1:
        return jsonify({'message': 'Admin user cannot be deleted'}), 403
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "DELETE FROM user WHERE user_id = %s"
        cursor.execute(query, (user_id,))

        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'User deleted successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'User not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
#movie table's endpoints

# 1. Create a Movie (Admin Only)
@app.route('/movies', methods=['POST'])
@token_required
def create_movie(current_user):
    """
    Create a Movie
    ---
    tags:
      - Movie
    security:
      - BearerAuth: []
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              title:
                type: string
                example: Inception
              description:
                type: string
                example: A mind-bending thriller about dreams within dreams.
              duration:
                type: integer
                example: 148
    responses:
      201:
        description: Movie created successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Movie created successfully
                movie_id:
                  type: integer
                  example: 1
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != 1:
        return jsonify({'message': 'Access denied'}), 403

    data = request.get_json()
    title = data.get('title')
    description = data.get('description')
    duration = data.get('duration')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "INSERT INTO movie (title, description, duration) VALUES (%s, %s, %s)"
        cursor.execute(query, (title, description, duration))
        connection.commit()

        movie_id = cursor.lastrowid
        cursor.close()
        connection.close()

        return jsonify({'message': 'Movie created successfully', 'movie_id': movie_id}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 2. Get All Movies (Any Logged-in User)
@app.route('/movies', methods=['GET'])
@token_required
def get_all_movies(current_user):
    """
    Get All Movies
    ---
    tags:
      - Movie
    security:
      - BearerAuth: []
    responses:
      200:
        description: List of all movies
        content:
          application/json:
            schema:
              type: object
              properties:
                movies:
                  type: array
                  items:
                    type: object
                    properties:
                      movie_id:
                        type: integer
                      title:
                        type: string
                      description:
                        type: string
                      duration:
                        type: integer
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "SELECT * FROM movie"
        cursor.execute(query)
        movies = cursor.fetchall()
        cursor.close()
        connection.close()

        movie_list = []
        for movie in movies:
            movie_list.append({
                'movie_id': movie[0],
                'title': movie[1],
                'description': movie[2],
                'duration': movie[3]
            })

        return jsonify({'movies': movie_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 3. Get a Single Movie by ID (Any Logged-in User)
@app.route('/movies/<int:movie_id>', methods=['GET'])
@token_required
def get_movie(current_user, movie_id):
    """
    Get a Single Movie by ID
    ---
    tags:
      - Movie
    security:
      - BearerAuth: []
    parameters:
      - name: movie_id
        in: path
        required: true
        description: ID of the movie
        schema:
          type: integer
    responses:
      200:
        description: Movie details
        content:
          application/json:
            schema:
              type: object
              properties:
                movie:
                  type: object
                  properties:
                    movie_id:
                      type: integer
                    title:
                      type: string
                    description:
                      type: string
                    duration:
                      type: integer
      404:
        description: Movie not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Movie not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "SELECT * FROM movie WHERE movie_id = %s"
        cursor.execute(query, (movie_id,))
        movie = cursor.fetchone()
        cursor.close()
        connection.close()

        if movie:
            movie_data = {
                'movie_id': movie[0],
                'title': movie[1],
                'description': movie[2],
                'duration': movie[3]
            }
            return jsonify({'movie': movie_data}), 200
        else:
            return jsonify({'message': 'Movie not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 4. Update a Movie (Admin Only)
@app.route('/movies/<int:movie_id>', methods=['PUT'])
@token_required
def update_movie(current_user, movie_id):
    """
    Update a Movie
    ---
    tags:
      - Movie
    security:
      - BearerAuth: []
    parameters:
      - name: movie_id
        in: path
        required: true
        description: ID of the movie
        schema:
          type: integer
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              title:
                type: string
                example: Updated Movie Title
              description:
                type: string
                example: Updated movie description.
              duration:
                type: integer
                example: 120
    responses:
      200:
        description: Movie updated successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Movie updated successfully
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      404:
        description: Movie not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Movie not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != 1:
        return jsonify({'message': 'Access denied'}), 403

    data = request.get_json()
    title = data.get('title')
    description = data.get('description')
    duration = data.get('duration')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "UPDATE movie SET title = %s, description = %s, duration = %s WHERE movie_id = %s"
        cursor.execute(query, (title, description, duration, movie_id))

        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'Movie updated successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Movie not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 5. Delete a Movie (Admin Only)
@app.route('/movies/<int:movie_id>', methods=['DELETE'])
@token_required
def delete_movie(current_user, movie_id):
    """
    Delete a Movie
    ---
    tags:
      - Movie
    security:
      - BearerAuth: []
    parameters:
      - name: movie_id
        in: path
        required: true
        description: ID of the movie
        schema:
          type: integer
    responses:
      200:
        description: Movie deleted successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Movie deleted successfully
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      404:
        description: Movie not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Movie not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != 1:
        return jsonify({'message': 'Access denied'}), 403

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "DELETE FROM movie WHERE movie_id = %s"
        cursor.execute(query, (movie_id,))

        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'Movie deleted successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Movie not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# genre table's endpoints

# 1. Create a Genre
@app.route('/genres', methods=['POST'])
@token_required
def create_genre(current_user):
    """
    Create a Genre
    ---
    tags:
      - Genre
    security:
      - BearerAuth: []
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              genre_name:
                type: string
                example: Action
    responses:
      201:
        description: Genre created successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Genre created successfully
                genre_id:
                  type: integer
                  example: 1
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != 1:
        return jsonify({'message': 'Access denied'}), 403
    
    data = request.get_json()
    genre_name = data.get('genre_name')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "INSERT INTO genre (genre_name) VALUES (%s)"
        cursor.execute(query, (genre_name,))
        connection.commit()

        genre_id = cursor.lastrowid
        cursor.close()
        connection.close()

        return jsonify({'message': 'Genre created successfully', 'genre_id': genre_id}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 2. Get All Genres
@app.route('/genres', methods=['GET'])
@token_required
def get_all_genres(current_user):
    """
    Get All Genres
    ---
    tags:
      - Genre
    security:
      - BearerAuth: []
    responses:
      200:
        description: List of all genres
        content:
          application/json:
            schema:
              type: object
              properties:
                genres:
                  type: array
                  items:
                    type: object
                    properties:
                      genre_id:
                        type: integer
                      genre_name:
                        type: string
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "SELECT * FROM genre"
        cursor.execute(query)
        genres = cursor.fetchall()
        cursor.close()
        connection.close()

        genre_list = [{'genre_id': genre[0], 'genre_name': genre[1]} for genre in genres]
        return jsonify({'genres': genre_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 3. Get a Single Genre by ID
@app.route('/genres/<int:genre_id>', methods=['GET'])
@token_required
def get_genre(current_user, genre_id):
    """
    Get a Single Genre by ID
    ---
    tags:
      - Genre
    security:
      - BearerAuth: []
    parameters:
      - name: genre_id
        in: path
        required: true
        description: ID of the genre
        schema:
          type: integer
    responses:
      200:
        description: Genre details
        content:
          application/json:
            schema:
              type: object
              properties:
                genre:
                  type: object
                  properties:
                    genre_id:
                      type: integer
                    genre_name:
                      type: string
      404:
        description: Genre not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Genre not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "SELECT * FROM genre WHERE genre_id = %s"
        cursor.execute(query, (genre_id,))
        genre = cursor.fetchone()
        cursor.close()
        connection.close()

        if genre:
            genre_data = {'genre_id': genre[0], 'genre_name': genre[1]}
            return jsonify({'genre': genre_data}), 200
        else:
            return jsonify({'message': 'Genre not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 4. Update a Genre
@app.route('/genres/<int:genre_id>', methods=['PUT'])
@token_required
def update_genre(current_user, genre_id):
    """
    Update a Genre
    ---
    tags:
      - Genre
    security:
      - BearerAuth: []
    parameters:
      - name: genre_id
        in: path
        required: true
        description: ID of the genre
        schema:
          type: integer
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              genre_name:
                type: string
                example: Adventure
    responses:
      200:
        description: Genre updated successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Genre updated successfully
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      404:
        description: Genre not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Genre not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != 1:
        return jsonify({'message': 'Access denied'}), 403
    
    data = request.get_json()
    genre_name = data.get('genre_name')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "UPDATE genre SET genre_name = %s WHERE genre_id = %s"
        cursor.execute(query, (genre_name, genre_id))
        
        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'Genre updated successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Genre not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 5. Delete a Genre
@app.route('/genres/<int:genre_id>', methods=['DELETE'])
@token_required
def delete_genre(current_user, genre_id):
    """
    Delete a Genre
    ---
    tags:
      - Genre
    security:
      - BearerAuth: []
    parameters:
      - name: genre_id
        in: path
        required: true
        description: ID of the genre
        schema:
          type: integer
    responses:
      200:
        description: Genre deleted successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Genre deleted successfully
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      404:
        description: Genre not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Genre not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != 1:
        return jsonify({'message': 'Access denied'}), 403
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "DELETE FROM genre WHERE genre_id = %s"
        cursor.execute(query, (genre_id,))

        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'Genre deleted successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Genre not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Movie-Genre Relationship Endpoints

# 1. Assign a Genre to a Movie
@app.route('/movie-genre', methods=['POST'])
@token_required
def assign_genre_to_movie(current_user):
    """
    Assign a Genre to a Movie
    ---
    tags:
      - Movie-Genre
    security:
      - BearerAuth: []
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              movie_id:
                type: integer
                example: 1
              genre_id:
                type: integer
                example: 2
    responses:
      201:
        description: Genre assigned to movie successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Genre assigned to movie successfully
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != 1:
        return jsonify({'message': 'Access denied'}), 403
    
    data = request.get_json()
    movie_id = data.get('movie_id')
    genre_id = data.get('genre_id')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "INSERT INTO movie_genre (movie_id, genre_id) VALUES (%s, %s)"
        cursor.execute(query, (movie_id, genre_id))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Genre assigned to movie successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 2. Get All Genres of a Movie
@app.route('/movies/<int:movie_id>/genres', methods=['GET'])
@token_required
def get_genres_of_movie(current_user, movie_id):
    """
    Get All Genres of a Movie
    ---
    tags:
      - Movie-Genre
    security:
      - BearerAuth: []
    parameters:
      - name: movie_id
        in: path
        required: true
        description: ID of the movie
        schema:
          type: integer
    responses:
      200:
        description: List of genres associated with the movie
        content:
          application/json:
            schema:
              type: object
              properties:
                genres:
                  type: array
                  items:
                    type: object
                    properties:
                      genre_id:
                        type: integer
                      genre_name:
                        type: string
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
            SELECT g.genre_id, g.genre_name 
            FROM genre g
            JOIN movie_genre mg ON g.genre_id = mg.genre_id
            WHERE mg.movie_id = %s
        """
        cursor.execute(query, (movie_id,))
        genres = cursor.fetchall()

        cursor.close()
        connection.close()

        genre_list = [{'genre_id': genre[0], 'genre_name': genre[1]} for genre in genres]
        return jsonify({'genres': genre_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 3. Get All Movies of a Genre
@app.route('/genres/<int:genre_id>/movies', methods=['GET'])
@token_required
def get_movies_of_genre(current_user, genre_id):
    """
    Get All Movies of a Genre
    ---
    tags:
      - Movie-Genre
    security:
      - BearerAuth: []
    parameters:
      - name: genre_id
        in: path
        required: true
        description: ID of the genre
        schema:
          type: integer
    responses:
      200:
        description: List of movies associated with the genre
        content:
          application/json:
            schema:
              type: object
              properties:
                movies:
                  type: array
                  items:
                    type: object
                    properties:
                      movie_id:
                        type: integer
                      title:
                        type: string
                      description:
                        type: string
                      duration:
                        type: string
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
            SELECT m.movie_id, m.title, m.description, m.duration 
            FROM movie m
            JOIN movie_genre mg ON m.movie_id = mg.movie_id
            WHERE mg.genre_id = %s
        """
        cursor.execute(query, (genre_id,))
        movies = cursor.fetchall()

        cursor.close()
        connection.close()

        movie_list = [{'movie_id': movie[0], 'title': movie[1], 'description': movie[2], 'duration': movie[3]} for movie in movies]
        return jsonify({'movies': movie_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 4. Remove a Genre from a Movie
@app.route('/movie-genre', methods=['DELETE'])
@token_required
def remove_genre_from_movie(current_user):
    """
    Remove a Genre from a Movie
    ---
    tags:
      - Movie-Genre
    security:
      - BearerAuth: []
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              movie_id:
                type: integer
                example: 1
              genre_id:
                type: integer
                example: 2
    responses:
      200:
        description: Genre removed from movie successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Genre removed from movie successfully
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      404:
        description: Movie-Genre relationship not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Movie-Genre relationship not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != 1:
        return jsonify({'message': 'Access denied'}), 403
    
    data = request.get_json()
    movie_id = data.get('movie_id')
    genre_id = data.get('genre_id')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "DELETE FROM movie_genre WHERE movie_id = %s AND genre_id = %s"
        cursor.execute(query, (movie_id, genre_id))

        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'Genre removed from movie successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Movie-Genre relationship not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Add a Rating
@app.route('/ratings', methods=['POST'])
@token_required
def add_rating(current_user):
    """
    Add a Rating
    ---
    tags:
      - Ratings
    security:
      - BearerAuth: []
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              movie_id:
                type: integer
                example: 1
              score:
                type: number
                format: float
                minimum: 0
                maximum: 10
                example: 8.5
    responses:
      201:
        description: Rating added successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Rating added successfully
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    data = request.get_json()
    movie_id = data.get('movie_id')
    score = data.get('score')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "INSERT INTO rating (user_id, movie_id, score) VALUES (%s, %s, %s)"
        cursor.execute(query, (current_user, movie_id, score))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Rating added successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Get All Ratings for a Movie
@app.route('/movies/<int:movie_id>/ratings', methods=['GET'])
@token_required
def get_ratings_for_movie(current_user, movie_id):
    """
    Get All Ratings for a Movie
    ---
    tags:
      - Ratings
    security:
      - BearerAuth: []
    parameters:
      - name: movie_id
        in: path
        required: true
        description: ID of the movie
        schema:
          type: integer
    responses:
      200:
        description: List of ratings for the specified movie
        content:
          application/json:
            schema:
              type: object
              properties:
                ratings:
                  type: array
                  items:
                    type: object
                    properties:
                      rating_id:
                        type: integer
                      user_id:
                        type: integer
                      score:
                        type: number
                        format: float
                      user_name:
                        type: string
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
            SELECT r.rating_id, r.user_id, r.score, u.user_name 
            FROM rating r
            JOIN user u ON r.user_id = u.user_id
            WHERE r.movie_id = %s
        """
        cursor.execute(query, (movie_id,))

        ratings = cursor.fetchall()
        cursor.close()
        connection.close()

        rating_list = [{'rating_id': rating[0], 'user_id': rating[1], 'score': rating[2], 'user_name': rating[3]} for rating in ratings]
        return jsonify({'ratings': rating_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Update a Rating (Only User's Own Rating)
@app.route('/ratings/<int:rating_id>', methods=['PUT'])
@token_required
def update_rating(current_user, rating_id):
    """
    Update a Rating (Only User's Own Rating)
    ---
    tags:
      - Ratings
    security:
      - BearerAuth: []
    parameters:
      - name: rating_id
        in: path
        required: true
        description: ID of the rating
        schema:
          type: integer
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              score:
                type: number
                format: float
                minimum: 0
                maximum: 10
                example: 9.0
    responses:
      200:
        description: Rating updated successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Rating updated successfully
      403:
        description: Unauthorized to update this rating
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Unauthorized to update this rating
      404:
        description: Rating not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Rating not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    data = request.get_json()
    score = data.get('score')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        # Verify the rating belongs to the current user
        query = "SELECT user_id FROM rating WHERE rating_id = %s"
        cursor.execute(query, (rating_id,))
        result = cursor.fetchone()

        if not result or result[0] != current_user:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Unauthorized to update this rating'}), 403

        # Update the rating
        query = "UPDATE rating SET score = %s WHERE rating_id = %s"
        cursor.execute(query, (score, rating_id))

        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'Rating updated successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Rating not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Delete a Rating (Only User's Own Rating)
@app.route('/ratings/<int:rating_id>', methods=['DELETE'])
@token_required
def delete_rating(current_user, rating_id):
    """
    Delete a Rating (Only User's Own Rating)
    ---
    tags:
      - Ratings
    security:
      - BearerAuth: []
    parameters:
      - name: rating_id
        in: path
        required: true
        description: ID of the rating
        schema:
          type: integer
    responses:
      200:
        description: Rating deleted successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Rating deleted successfully
      403:
        description: Unauthorized to delete this rating
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Unauthorized to delete this rating
      404:
        description: Rating not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Rating not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        # Verify the rating belongs to the current user
        query = "SELECT user_id FROM rating WHERE rating_id = %s"
        cursor.execute(query, (rating_id,))
        result = cursor.fetchone()

        if not result or result[0] != current_user:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Unauthorized to delete this rating'}), 403

        # Delete the rating
        query = "DELETE FROM rating WHERE rating_id = %s"
        cursor.execute(query, (rating_id,))

        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'Rating deleted successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Rating not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Add a Review
@app.route('/reviews', methods=['POST'])
@token_required
def add_review(current_user):
    """
    Add a Review
    ---
    tags:
      - Reviews
    security:
      - BearerAuth: []
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              movie_id:
                type: integer
                example: 5
              review_text:
                type: string
                example: "Amazing movie with great visuals!"
    responses:
      201:
        description: Review added successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Review added successfully
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    data = request.get_json()
    movie_id = data.get('movie_id')
    review_text = data.get('review_text')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "INSERT INTO review (user_id, movie_id, review_text) VALUES (%s, %s, %s)"
        cursor.execute(query, (current_user, movie_id, review_text))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Review added successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Get All Reviews for a Movie
@app.route('/movies/<int:movie_id>/reviews', methods=['GET'])
@token_required
def get_reviews_for_movie(current_user, movie_id):
    """
    Get All Reviews for a Movie
    ---
    tags:
      - Reviews
    security:
      - BearerAuth: []
    parameters:
      - name: movie_id
        in: path
        required: true
        description: ID of the movie
        schema:
          type: integer
    responses:
      200:
        description: List of reviews for the specified movie
        content:
          application/json:
            schema:
              type: object
              properties:
                reviews:
                  type: array
                  items:
                    type: object
                    properties:
                      review_id:
                        type: integer
                      user_id:
                        type: integer
                      review_text:
                        type: string
                      user_name:
                        type: string
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
            SELECT r.review_id, r.user_id, r.review_text, u.user_name 
            FROM review r
            JOIN user u ON r.user_id = u.user_id
            WHERE r.movie_id = %s
        """
        cursor.execute(query, (movie_id,))

        reviews = cursor.fetchall()
        cursor.close()
        connection.close()

        review_list = [{'review_id': review[0], 'user_id': review[1], 'review_text': review[2], 'user_name': review[3]} for review in reviews]
        return jsonify({'reviews': review_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Update a Review (Only User's Own Review)
@app.route('/reviews/<int:review_id>', methods=['PUT'])
@token_required
def update_review(current_user, review_id):
    """
    Update a Review
    ---
    tags:
      - Reviews
    security:
      - BearerAuth: []
    parameters:
      - name: review_id
        in: path
        required: true
        description: ID of the review
        schema:
          type: integer
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              review_text:
                type: string
                example: "Updated review text here."
    responses:
      200:
        description: Review updated successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Review updated successfully
      403:
        description: Unauthorized to update this review
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Unauthorized to update this review
      404:
        description: Review not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Review not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    data = request.get_json()
    review_text = data.get('review_text')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        # Verify the review belongs to the current user
        query = "SELECT user_id FROM review WHERE review_id = %s"
        cursor.execute(query, (review_id,))
        result = cursor.fetchone()

        if not result or result[0] != current_user:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Unauthorized to update this review'}), 403

        # Update the review
        query = "UPDATE review SET review_text = %s WHERE review_id = %s"
        cursor.execute(query, (review_text, review_id))

        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'Review updated successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Review not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Delete a Review (Only User's Own Review)
@app.route('/reviews/<int:review_id>', methods=['DELETE'])
@token_required
def delete_review(current_user, review_id):
    """
    Delete a Review
    ---
    tags:
      - Reviews
    security:
      - BearerAuth: []
    parameters:
      - name: review_id
        in: path
        required: true
        description: ID of the review
        schema:
          type: integer
    responses:
      200:
        description: Review deleted successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Review deleted successfully
      403:
        description: Unauthorized to delete this review
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Unauthorized to delete this review
      404:
        description: Review not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Review not found
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        # Verify the review belongs to the current user
        query = "SELECT user_id FROM review WHERE review_id = %s"
        cursor.execute(query, (review_id,))
        result = cursor.fetchone()

        if not result or result[0] != current_user:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Unauthorized to delete this review'}), 403

        # Delete the review
        query = "DELETE FROM review WHERE review_id = %s"
        cursor.execute(query, (review_id,))

        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'Review deleted successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Review not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

# Add to Watch History
@app.route('/watch-history', methods=['POST'])
@token_required
def add_to_watch_history(current_user):
    """
    Add to Watch History
    ---
    tags:
      - Watch History
    security:
      - BearerAuth: []
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              movie_id:
                type: integer
                example: 10
    responses:
      201:
        description: Movie added to watch history
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Movie added to watch history
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    data = request.get_json()
    movie_id = data.get('movie_id')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "INSERT INTO watch_history (user_id, movie_id) VALUES (%s, %s)"
        cursor.execute(query, (current_user, movie_id))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Movie added to watch history'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Get Watch History for a User
@app.route('/watch-history', methods=['GET'])
@token_required
def get_watch_history_for_user(current_user):
    """
    Get Watch History for a User
    ---
    tags:
      - Watch History
    security:
      - BearerAuth: []
    responses:
      200:
        description: List of movies in the user's watch history
        content:
          application/json:
            schema:
              type: object
              properties:
                watch_history:
                  type: array
                  items:
                    type: object
                    properties:
                      history_id:
                        type: integer
                      movie_id:
                        type: integer
                      title:
                        type: string
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
            SELECT h.history_id, h.movie_id, m.title 
            FROM watch_history h
            JOIN movie m ON h.movie_id = m.movie_id
            WHERE h.user_id = %s
        """
        cursor.execute(query, (current_user,))
        
        watch_history = cursor.fetchall()
        cursor.close()
        connection.close()

        history_list = [{'history_id': history[0], 'movie_id': history[1], 'title': history[2]} for history in watch_history]
        return jsonify({'watch_history': history_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Add a Recommendation (Admin Only)
@app.route('/recommendations', methods=['POST'])
@token_required
def add_recommendation(current_user):
    """
    Add a Recommendation (Admin Only)
    ---
    tags:
      - Recommendations
    security:
      - BearerAuth: []
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              user_id:
                type: integer
                description: ID of the user to receive the recommendation
              movie_id:
                type: integer
                description: ID of the movie to be recommended
            required:
              - user_id
              - movie_id
    responses:
      201:
        description: Recommendation added successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Recommendation added successfully
      400:
        description: User has already watched this movie
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: User has already watched this movie. Recommendation not added.
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != 1:  # Admin user check
        return jsonify({'message': 'Access denied'}), 403

    data = request.get_json()
    user_id = data.get('user_id')
    movie_id = data.get('movie_id')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        # Check if the user has already watched the movie
        check_query = "SELECT history_id FROM watch_history WHERE user_id = %s AND movie_id = %s"
        cursor.execute(check_query, (user_id, movie_id))
        watched = cursor.fetchone()

        if watched:
            return jsonify({'message': 'User has already watched this movie. Recommendation not added.'}), 400

        # Add the recommendation
        query = "INSERT INTO recommendation (user_id, movie_id) VALUES (%s, %s)"
        cursor.execute(query, (user_id, movie_id))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Recommendation added successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Get Recommendations for the Current User
@app.route('/recommendations', methods=['GET'])
@token_required
def get_recommendations_for_user(current_user):
    """
    Get Recommendations for the Current User
    ---
    tags:
      - Recommendations
    security:
      - BearerAuth: []
    responses:
      200:
        description: List of recommendations for the current user
        content:
          application/json:
            schema:
              type: object
              properties:
                recommendations:
                  type: array
                  items:
                    type: object
                    properties:
                      recommendation_id:
                        type: integer
                      movie_id:
                        type: integer
                      title:
                        type: string
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
            SELECT r.recommendation_id, r.movie_id, m.title 
            FROM recommendation r
            JOIN movie m ON r.movie_id = m.movie_id
            WHERE r.user_id = %s
        """
        cursor.execute(query, (current_user,))
        
        recommendations = cursor.fetchall()
        cursor.close()
        connection.close()

        recommendation_list = [{'recommendation_id': rec[0], 'movie_id': rec[1], 'title': rec[2]} for rec in recommendations]
        return jsonify({'recommendations': recommendation_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Admin-Only: Delete a Recommendation for a Specific User
@app.route('/recommendations/<int:user_id>/<int:movie_id>', methods=['DELETE'])
@token_required
def delete_recommendation(current_user, user_id, movie_id):
    """
    Delete a Recommendation for a Specific User (Admin Only)
    ---
    tags:
      - Recommendations
    security:
      - BearerAuth: []
    parameters:
      - name: user_id
        in: path
        required: true
        schema:
          type: integer
        description: ID of the user whose recommendation is being deleted
      - name: movie_id
        in: path
        required: true
        schema:
          type: integer
        description: ID of the movie being removed from recommendations
    responses:
      200:
        description: Recommendation deleted successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Recommendation deleted successfully
      404:
        description: Recommendation not found
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Recommendation not found
      403:
        description: Access denied
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Access denied
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    if current_user != 1:  # Admin user check
        return jsonify({'message': 'Access denied'}), 403

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "DELETE FROM recommendation WHERE user_id = %s AND movie_id = %s"
        cursor.execute(query, (user_id, movie_id))
        
        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({'message': 'Recommendation deleted successfully'}), 200
        else:
            cursor.close()
            connection.close()
            return jsonify({'message': 'Recommendation not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/register', methods=['POST'])
def register():
    """
    Register a New User
    ---
    tags:
      - Authentication
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              user_name:
                type: string
                description: Username of the new user
              email:
                type: string
                description: Email address of the new user
              password:
                type: string
                description: Password for the new user
            required:
              - user_name
              - email
              - password
    responses:
      201:
        description: User registered successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: User registered successfully
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    data = request.get_json()
    user_name = data.get('user_name')
    email = data.get('email')
    password = data.get('password')

    hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = "INSERT INTO user (user_name, email, password) VALUES (%s, %s, %s)"
        cursor.execute(query, (user_name, email, hashed_password))

        connection.commit()
        cursor.close()
        connection.close()

        return jsonify({'message': 'User registered successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/login', methods=['POST'])
def login():
    """
    User Login
    ---
    tags:
      - Authentication
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              email:
                type: string
                description: Email address of the user
              password:
                type: string
                description: Password of the user
            required:
              - email
              - password
    responses:
      200:
        description: User authenticated successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                token:
                  type: string
                  description: JWT token for authentication
      401:
        description: Invalid credentials
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: Invalid credentials
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    try:
        connection = create_connection()
        cursor = connection.cursor(dictionary=True)

        query = "SELECT * FROM user WHERE email = %s"
        cursor.execute(query, (email,))
        user = cursor.fetchone()

        cursor.close()
        connection.close()

        if user and bcrypt.check_password_hash(user['password'], password):
            # Generate JWT token
            token = jwt.encode(
                {
                    'user_id': user['user_id'],
                    'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
                },
                app.config['SECRET_KEY'],
                algorithm='HS256'
            )
            return jsonify({'token': token}), 200
        else:
            return jsonify({'message': 'Invalid credentials'}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# complex queries
@app.route('/movies/filter', methods=['GET'])
@token_required
def filter_movies(current_user):
    """
    Filter Movies by Genre, Duration, and Rating
    ---
    tags:
      - Movies
    security:
      - BearerAuth: []
    parameters:
      - name: genre_name
        in: query
        required: true
        schema:
          type: string
        description: Name of the genre to filter by
      - name: min_duration
        in: query
        required: false
        schema:
          type: integer
          default: 0
        description: Minimum duration of the movie in minutes
      - name: max_duration
        in: query
        required: false
        schema:
          type: integer
          default: 1000
        description: Maximum duration of the movie in minutes
      - name: min_rating
        in: query
        required: false
        schema:
          type: number
          format: float
          default: 0
        description: Minimum average rating of the movie
    responses:
      200:
        description: List of movies matching the filter criteria
        content:
          application/json:
            schema:
              type: object
              properties:
                movies:
                  type: array
                  items:
                    type: object
                    properties:
                      movie_id:
                        type: integer
                      title:
                        type: string
                      description:
                        type: string
                      duration:
                        type: integer
                      avg_rating:
                        type: number
                        format: float
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    data = request.get_json()  
    genre_name = data.get('genre_name')
    min_duration = data.get('min_duration')
    max_duration = data.get('max_duration')
    min_rating = data.get('min_rating')


    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
            SELECT m.movie_id, m.title, m.description, m.duration, 
                   COALESCE(AVG(r.score), 0) AS avg_rating
            FROM movie m
            JOIN movie_genre mg ON m.movie_id = mg.movie_id
            JOIN genre g ON mg.genre_id = g.genre_id
            LEFT JOIN rating r ON m.movie_id = r.movie_id
            WHERE g.genre_name = %s
            AND m.duration BETWEEN %s AND %s
            GROUP BY m.movie_id
            HAVING avg_rating >= %s
        """
        cursor.execute(query, (genre_name, min_duration, max_duration, min_rating))
        movies = cursor.fetchall()

        cursor.close()
        connection.close()

        movie_list = [{'movie_id': movie[0], 'title': movie[1], 'description': movie[2],
                       'duration': movie[3], 'avg_rating': float(movie[4])} for movie in movies]
        return jsonify({'movies': movie_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


    
@app.route('/movies/top', methods=['GET'])
@token_required
def top_movies_by_genre(current_user):
    """
    Get Top Movies by Genre
    ---
    tags:
      - Movies
    security:
      - BearerAuth: []
    parameters:
      - name: genre_name
        in: query
        required: true
        schema:
          type: string
        description: Name of the genre to get top movies
      - name: limit
        in: query
        required: false
        schema:
          type: integer
          default: 10
        description: Number of top movies to return
    responses:
      200:
        description: List of top movies for the specified genre
        content:
          application/json:
            schema:
              type: object
              properties:
                movies:
                  type: array
                  items:
                    type: object
                    properties:
                      movie_id:
                        type: integer
                      title:
                        type: string
                      description:
                        type: string
                      rating:
                        type: number
                        format: float
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    data = request.get_json()  
    genre_name = data.get('genre_name')
    limit = data.get('limit')

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
            SELECT m.movie_id, m.title, m.description, COALESCE(AVG(r.score), 0) AS avg_rating
            FROM movie m
            JOIN movie_genre mg ON m.movie_id = mg.movie_id
            JOIN genre g ON mg.genre_id = g.genre_id
            LEFT JOIN rating r ON m.movie_id = r.movie_id
            WHERE g.genre_name = %s
            ORDER BY avg_rating DESC
            LIMIT %s
        """
        cursor.execute(query, (genre_name, limit))
        movies = cursor.fetchall()

        cursor.close()
        connection.close()

        movie_list = [{'movie_id': movie[0], 'title': movie[1], 'description': movie[2],
                       'rating': movie[3]} for movie in movies]
        return jsonify({'movies': movie_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/genres/statistics', methods=['GET'])
@token_required
def genre_statistics(current_user):
    """
    Get Genre Statistics
    ---
    tags:
      - Genres
    security:
      - BearerAuth: []
    responses:
      200:
        description: Statistics for each genre, including movie count and average rating
        content:
          application/json:
            schema:
              type: object
              properties:
                statistics:
                  type: array
                  items:
                    type: object
                    properties:
                      genre_name:
                        type: string
                      movie_count:
                        type: integer
                      avg_rating:
                        type: number
                        format: float
                        nullable: true
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query ="""
            SELECT g.genre_name, COUNT(m.movie_id) AS movie_count, AVG(r.score) AS avg_rating
            FROM genre g
            LEFT JOIN movie_genre mg ON g.genre_id = mg.genre_id
            LEFT JOIN movie m ON mg.movie_id = m.movie_id
            LEFT JOIN rating r ON m.movie_id = r.movie_id
            GROUP BY g.genre_name
            ORDER BY movie_count DESC
        """ 

        cursor.execute(query)
        stats = cursor.fetchall()

        cursor.close()
        connection.close()

        genre_stats = [{'genre_name': stat[0], 'movie_count': stat[1], 'avg_rating': round(stat[2], 2) if stat[2] else None}
                       for stat in stats]
        return jsonify({'statistics': genre_stats}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/genres/top-rated-movie', methods=['GET'])
@token_required
def get_genres_of_top_rated_movie(current_user):
    """
    Get Genres of the Top-Rated Movie
    ---
    tags:
      - Genres
    security:
      - BearerAuth: []
    responses:
      200:
        description: List of genres associated with the top-rated movie
        content:
          application/json:
            schema:
              type: object
              properties:
                genres:
                  type: array
                  items:
                    type: object
                    properties:
                      genre_id:
                        type: integer
                      genre_name:
                        type: string
      500:
        description: Internal server error
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: Database connection error
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
            SELECT DISTINCT g.genre_id, g.genre_name
            FROM genre g
            JOIN movie_genre mg ON g.genre_id = mg.genre_id
            WHERE mg.movie_id IN (
                SELECT m.movie_id
                FROM movie m
                LEFT JOIN rating r ON m.movie_id = r.movie_id
                GROUP BY m.movie_id
                HAVING AVG(COALESCE(r.score, 0)) = (
                    SELECT MAX(avg_rating)
                    FROM (
                        SELECT AVG(COALESCE(r.score, 0)) AS avg_rating
                        FROM movie m
                        LEFT JOIN rating r ON m.movie_id = r.movie_id
                        GROUP BY m.movie_id
                    ) AS subquery
                )
            )
        """
        cursor.execute(query)
        genres = cursor.fetchall()

        cursor.close()
        connection.close()

        genre_list = [{'genre_id': genre[0], 'genre_name': genre[1]} for genre in genres]
        return jsonify({'genres': genre_list}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)