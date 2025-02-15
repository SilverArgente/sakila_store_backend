from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from datetime import datetime

app = Flask(__name__)

# Configure the database URI (e.g., SQLite for simplicity)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sakila_master.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

with app.app_context():
    # Example: Fetch all rows from a table
    result = db.session.execute(text("SELECT * FROM film"))
    rows = result.fetchall()  # List of tuples

    for row in rows:
        print(row)  # Each row is a tuple of column values


@app.route("/members")
def members():
    return rows


if __name__ == "__main__":
    app.run(debug=True)