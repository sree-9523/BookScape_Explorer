import requests
import mysql.connector
import json
from mysql.connector import Error


def create_database_schema(cursor):
    """Create the complete database schema with all required tables"""

    # Drop existing tables in correct order
    cursor.execute("""
        DROP TABLE IF EXISTS  
        book_categories, 
        book_authors, 
        industry_identifiers, 
        books, 
        publishers, 
        authors, 
        categories
    """)

    # Create Publishers table
    cursor.execute("""
        CREATE TABLE publishers (
            publisher_id SERIAL PRIMARY KEY,
            publisher_name VARCHAR(255) NOT NULL UNIQUE
        )
    """)

    # Create Authors table
    cursor.execute("""
        CREATE TABLE authors (
            author_id SERIAL PRIMARY KEY,
            author_name VARCHAR(255) NOT NULL UNIQUE
        )
    """)

    # Create Categories table
    cursor.execute("""
        CREATE TABLE categories (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(255) NOT NULL UNIQUE
        )
    """)

    # Create main Books table - now with categories column
    cursor.execute("""
        CREATE TABLE books (
            book_id VARCHAR(50) PRIMARY KEY,
            search_key VARCHAR(255),
            book_title VARCHAR(500) NOT NULL,
            book_subtitle TEXT,
            book_description TEXT,
            book_authors TEXT,
            categories TEXT,
            text_readingModes BOOLEAN DEFAULT false,
            image_readingModes BOOLEAN DEFAULT false,
            pageCount INTEGER,
            language VARCHAR(10),
            publisher_id INTEGER REFERENCES publishers(publisher_id),
            publication_year INTEGER,
            ratingsCount INTEGER DEFAULT 0,
            averageRating DECIMAL(3,2),
            isEbook BOOLEAN DEFAULT false,
            amount_listPrice DECIMAL(10,2),
            currencyCode_listPrice VARCHAR(3),
            amount_retailPrice DECIMAL(10,2),
            currencyCode_retailPrice VARCHAR(3),
            buyLink TEXT,
            imageLinks TEXT,
            country VARCHAR(50),
            saleability VARCHAR(50)
        )
    """)

    # Create Book-Author mapping table
    cursor.execute("""
        CREATE TABLE book_authors (
            book_id VARCHAR(50) REFERENCES books(book_id) ON DELETE CASCADE,
            author_id INTEGER REFERENCES authors(author_id) ON DELETE CASCADE,
            PRIMARY KEY (book_id, author_id)
        )
    """)

    # Create Book-Category mapping table
    cursor.execute("""
        CREATE TABLE book_categories (
            book_id VARCHAR(50) REFERENCES books(book_id) ON DELETE CASCADE,
            category_id INTEGER REFERENCES categories(category_id) ON DELETE CASCADE,
            PRIMARY KEY (book_id, category_id)
        )
    """)

    # Create Industry Identifiers table
    cursor.execute("""
        CREATE TABLE industry_identifiers (
            identifier_id SERIAL PRIMARY KEY,
            book_id VARCHAR(50) REFERENCES books(book_id) ON DELETE CASCADE,
            identifier_type VARCHAR(20),
            identifier_value VARCHAR(50)
        )
    """)

    # Create indexes for query optimization
    cursor.execute("CREATE INDEX idx_publication_year ON books(publication_year)")
    cursor.execute("CREATE INDEX idx_pagecount ON books(pageCount)")
    cursor.execute("CREATE INDEX idx_rating ON books(averageRating)")
    cursor.execute("CREATE INDEX idx_publisher ON books(publisher_id)")
    cursor.execute("CREATE INDEX idx_isebook ON books(isEbook)")

    print("Database schema created successfully")


def insert_publisher(cursor, publisher_name):
    try:
        cursor.execute("INSERT IGNORE INTO publishers (publisher_name) VALUES (%s)", (publisher_name,))
        cursor.execute("SELECT publisher_id FROM publishers WHERE publisher_name = %s", (publisher_name,))
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error handling publisher {publisher_name}: {e}")
        return None


def insert_author(cursor, author_name):
    try:
        cursor.execute("INSERT IGNORE INTO authors (author_name) VALUES (%s)", (author_name,))
        cursor.execute("SELECT author_id FROM authors WHERE author_name = %s", (author_name,))
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error handling author {author_name}: {e}")
        return None


def insert_category(cursor, category_name):
    try:
        cursor.execute("INSERT IGNORE INTO categories (category_name) VALUES (%s)", (category_name,))
        cursor.execute("SELECT category_id FROM categories WHERE category_name = %s", (category_name,))
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error handling category {category_name}: {e}")
        return None


# Function to scrape books data from Google API
def scrap(query, api_key, max_results):
    url = "https://www.googleapis.com/books/v1/volumes"
    results = []
    max_results_per_request = 40
    for start in range(0, max_results, max_results_per_request):
        # Parameters for the request
        params = {
            "q": query,
            "startIndex": start,
            "maxResults": min(max_results_per_request, max_results - start),
            "key": api_key
        }

        # Make the API request
        response = requests.get(url, params=params)
        data = response.json()

        # Append results
        results.extend(data.get("items", []))

    return results


def process_book(book_item, search_key, cursor):
    """Process a single book item and insert into database with all relationships"""
    try:
        volume_info = book_item.get("volumeInfo", {})
        sale_info = book_item.get("saleInfo", {})

        # Get publisher
        publisher_name = volume_info.get("publisher", "Unknown")
        publisher_id = insert_publisher(cursor, publisher_name)

        # Process authors
        authors = volume_info.get("authors", [])
        authors_str = ", ".join(authors) if authors else "NA"

        # Process categories
        categories = volume_info.get("categories", [])
        categories_str = ", ".join(categories) if categories else "NA"

        # Extract year
        published_date = volume_info.get("publishedDate", "")
        try:
            year = int(published_date.split('-')[0])
            if not (1800 <= year <= 2024):
                year = None
        except:
            year = None

        # Prepare book data
        book_data = {
            "book_id": book_item.get("id"),
            "search_key": search_key,
            "book_title": volume_info.get("title", "NA"),
            "book_subtitle": volume_info.get("subtitle"),
            "book_description": volume_info.get("description"),
            "book_authors": authors_str,
            "categories": categories_str,
            "text_readingModes": volume_info.get("readingModes", {}).get("text", False),
            "image_readingModes": volume_info.get("readingModes", {}).get("image", False),
            "pageCount": volume_info.get("pageCount"),
            "language": volume_info.get("language"),
            "publisher_id": publisher_id,
            "publication_year": year,
            "ratingsCount": volume_info.get("ratingsCount"),
            "averageRating": volume_info.get("averageRating"),
            "isEbook": sale_info.get("isEbook", False),
            "amount_listPrice": sale_info.get("listPrice", {}).get("amount"),
            "currencyCode_listPrice": sale_info.get("listPrice", {}).get("currencyCode"),
            "amount_retailPrice": sale_info.get("retailPrice", {}).get("amount"),
            "currencyCode_retailPrice": sale_info.get("retailPrice", {}).get("currencyCode"),
            "buyLink": sale_info.get("buyLink"),
            "imageLinks": json.dumps(volume_info.get("imageLinks", {})),
            "country": sale_info.get("country", "NA"),
            "saleability": sale_info.get("saleability", "NA")
        }

        # Remove None values
        book_data = {k: v for k, v in book_data.items() if v is not None}

        # Insert book
        columns = ", ".join(book_data.keys())
        placeholders = ", ".join(["%s"] * len(book_data))
        insert_query = f"INSERT INTO books ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(book_data.values()))

        # Processing authors
        for author_name in authors:
            if author_name:  # Make sure author name is not empty
                author_id = insert_author(cursor, author_name)
                if author_id:
                    cursor.execute("""
                        INSERT INTO book_authors (book_id, author_id)
                        VALUES (%s, %s)
                    """, (book_data["book_id"], author_id))

        # Processing categories
        for category_name in categories:
            category_id = insert_category(cursor, category_name)
            if category_id:
                cursor.execute("""
                    INSERT IGNORE INTO book_categories (book_id, category_id)
                    VALUES (%s, %s)
                """, (book_data["book_id"], category_id))

        # Processing industry identifiers
        identifiers = volume_info.get("industryIdentifiers", [])
        for identifier in identifiers:
            cursor.execute("""
                INSERT INTO industry_identifiers (book_id, identifier_type, identifier_value)
                VALUES (%s, %s, %s)
            """, (book_data["book_id"], identifier.get("type"), identifier.get("identifier")))

        return True

    except Exception as e:
        print(f"Error processing book {book_item.get('id', 'unknown')}: {e}")
        return False


def main():
    api_key = 'The API key'
    search_keys = [
        "Python programming",
        "Data Science",
        "Machine Learning",
        "Web Development",
        "Economics",
        "Cooking Books",
        "English Literature",
        "Human Psychology",
        "Physics",
        "Business"
    ]

    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='******',
            database='bookscape_explorer'
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Creating the database schema
            create_database_schema(cursor)

            # Process each search key
            for search_key in search_keys:
                print(f"Processing search key: {search_key}")
                books_data = scrap(search_key, api_key, 500)

                successful_imports = 0
                for book_item in books_data:
                    if process_book(book_item, search_key, cursor):
                        successful_imports += 1
                        connection.commit()

                print(f"Completed {search_key}: {successful_imports} books imported")

    except Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"General error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")


if __name__ == "__main__":
    main()
