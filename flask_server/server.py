from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from sqlalchemy import text
from dataclasses import asdict
from datetime import datetime

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*", 
                                "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                                "allow_headers": ["Content-Type", "Authorization"]}})

# Configure the database URI (e.g., SQLite for simplicity)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sakila.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

data = {}
customer_data = []

with app.app_context():

    # Top 5 rented films
    top_films = db.session.execute(text("select film.title, film.description, film.release_year, film.rating, film.special_features, COUNT(film.title) as rented from rental \
                                        inner join inventory on rental.inventory_id = inventory.inventory_id \
                                        inner join film on film.film_id = inventory.film_id \
                                        group by inventory.film_id, film.title, film.description, film.release_year, film.rating, film.special_features order by rented desc limit 5;"))
    
    film_rows = top_films.fetchall()
    film_json = [dict(zip(top_films.keys(), row)) for row in film_rows]
    data["top_films"] = film_json

    # Top 5 actors
    top_actors = db.session.execute(text("select film_actor.actor_id, actor.first_name, actor.last_name, COUNT(film_actor.film_id) as movies from film_actor \
                                          inner join film on film_actor.film_id = film.film_id \
                                          inner join actor on film_actor.actor_id = actor.actor_id \
                                          group by film_actor.actor_id order by movies desc limit 5;"))
    
    actor_rows = top_actors.fetchall()
    actor_json = [dict(zip(top_actors.keys(), row)) for row in actor_rows]
    data["top_actors"] = actor_json

    # Get actor_id's of top 5 actors for next query
    top_five_actor_ids = [] 

    for i in range(5):
        top_five_actor_ids.append(data["top_actors"][i]["actor_id"])


    # Top 5 films for top 5 actors
    arr = []
    i = 0

    for id in top_five_actor_ids:
        
        query = "select inventory.film_id, film_list.title, COUNT(film_list.title) as rented from rental \
                                                inner join inventory on rental.inventory_id = inventory.inventory_id \
                                                inner join film_list on inventory.film_id = FID \
                                                inner join film_actor on film_actor.film_id = FID where film_actor.actor_id = {} \
                                                group by inventory.film_id, film_list.title order by rented desc limit 5;"
        top_actor_film = db.session.execute(text(query.format(id)))
        actor_film_rows = top_actor_film.fetchall()
        actor_film_json = [dict(zip(top_actor_film.keys(), row)) for row in actor_film_rows]
        arr.append(actor_film_json)
        i += 1

    data["top_actor_films"] = arr

    customers = db.session.execute(text("select * from customer;"))
    
    customer_rows = customers.fetchall()
    customer_json = [dict(zip(customers.keys(), row)) for row in customer_rows]
    customer_data = customer_json
    print(customer_json)


@app.route("/top_5")
def members():
    return jsonify(data)

@app.route("/customers")
def get_customers():
    try:

        return customer_data
        '''
        query = "SELECT * FROM customer;"

        customers = db.session.execute(text(query))
        customer_rows = customers.fetchall()
        customer_list = [dict(row) for row in customer_rows]

        return jsonify({"customer": customer_list})'''
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/film_search", methods=["GET"])
def film_search():
    query = request.args.get("query", "").lower()  # Get the search query
    search_type = request.args.get("type", "Film Name").lower()  # Get the search type, default to "Film Name"

    if not query:
        return jsonify({"error": "No search query provided"}), 400

    search_query = f"%{query}%"  # Format for LIKE search

    # Based on search type, execute different queries
    if search_type == "film name":
        result = db.session.execute(
            text("SELECT title, description, release_year, rating FROM film WHERE LOWER(title) LIKE :query"),
            {"query": search_query}
        )

    elif search_type == "actor name":
        result = db.session.execute(
            text("SELECT film.title, actor.first_name, actor.last_name FROM film \
                  INNER JOIN film_actor ON film.film_id = film_actor.film_id \
                  INNER JOIN actor ON film_actor.actor_id = actor.actor_id \
                  WHERE LOWER(actor.first_name) LIKE :query OR LOWER(actor.last_name) LIKE :query"),
            {"query": search_query}
        )

    elif search_type == "genre":
        result = db.session.execute(
            text("SELECT film.title, category.name FROM film \
                  INNER JOIN film_category ON film.film_id = film_category.film_id \
                  INNER JOIN category ON film_category.category_id = category.category_id \
                  WHERE LOWER(category.name) LIKE :query"),
            {"query": search_query}
        )

    else:
        return jsonify({"error": "Invalid search type"}), 400

    result_rows = result.fetchall()
    result_json = [dict(zip(result.keys(), row)) for row in result_rows]

    return jsonify({"results": result_json})


if __name__ == "__main__":
    app.run(debug=True)
