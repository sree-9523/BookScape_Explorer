# 📚 *BookScape Explorer - Book Data Analytics Project*
## **Introduction**
BookScape Explorer is an advanced data analytics project that brings together the power of the Google Books API and data visualization to create a comprehensive book analysis platform. This project consists of two main components: a data extraction system and an interactive analytics dashboard.

## What Does This Project Do?
The project serves two main purposes:
  1. **Data Collection:** It harvests detailed book information from Google Books API across multiple categories like Programming, Data Science, Literature, etc.
  2. **Data Analysis:** It provides an interactive dashboard where users can explore and analyze this book data through various visualizations and metrics.

## Project Components
#### 1. Book_Data.py (Data Extraction)
This script handles the data collection and database management portion of the project.

 * API Integration:
   - Connects to Google Books API
   - Fetches comprehensive book data
   - Handles API rate limiting and pagination
   - Processes JSON responses

 * Database Management:
   - Creates a normalized MySQL database structure
   - Handles data cleaning and validation
   - Manages relationships between different entities (authors, publishers, etc.)
   - Implements error handling for database operations

  * Data Processing:
    - Extracts relevant information from API responses
    - Normalizes data across different tables
    - Handles missing or inconsistent data
    - Creates proper relationships between different data entities

#### 2. Streamlit_Application.py (Analytics Dashboard)
This is the visualization and analysis component that provides an interactive web interface. Features include:

  * Search and Discovery:
    - Full-text search across all books
    - Filter by various parameters
    - Sort and organize results
  * Analysis Views:
      1. Book Format Analysis
         - Compare eBooks vs Physical Books
         - Analyze format preferences
         - Price comparison between formats
      2. Publisher Analysis
         - View top publishers by book count
         - Analyze publisher ratings
         - Track publishing trends
      3. Price Analysis
         - Track price distributions
         - Analyze discounts
         - Compare prices across categories
      4. Content Analysis
         - Page count distributions
         - Category-wise analysis
         - Language distribution
      5. Author Analysis
         - Track prolific authors
         - Analyze publication patterns
         - Cross-publisher relationship

## Project Set Up
  1. **Pre-requisites**
        - Python 3.8 or higher installed
        - MySQL Server installed and running
        - A Google Books API key
        - Git installed (for cloning the repository)
    
  2. **Installation**
        - Clone Repository
        ```python
        git clone https://github.com/sree-9523/BookScape_Explorer.git
        cd BookScape_Explorer
        ```
        - Use `pip install requirements.txt` to install dependencies
   
  3. **Database Setup**
        - Create Database in SQL Workbench `CREATE DATABASE bookscape_explorer;`
        - Configure Connection: Update database connection parameters in the Python files:
        ```python     
        connection = mysql.connector.connect(
          host='127.0.0.1',
          user='your_username',
          password='your_password',
          database='bookscape_explorer'
        )
        ```
  4. **API Configuration**
       - Replace the API key in Book_Data.py `api_key = 'your_google_books_api_key'`

## Project Execution
  1. **Data Collection**
     - Run the data extraction script 'Book_Data.py'
     - This will:
         * Create all necessary database tables
         * Fetch book data from Google Books API
         * Process and store the data
      
  2. **Launch Dashboard**
     - Start the analytics dashboard with 'streamlit run Streamlit_Application.py'
     - This will:
         * Launch the web interface
         * Connect to your database
         * Display all analysis options

## Database Structure
  - **Main Tables**
    1. books:
       - Primary book information
       - Pricing details
       - Publication information
       - Rating and review data
    2. publishers:
       - Publisher information
       - Publishing statistics
    3. authors:
       - Author details
       - Publication history
    4. categories:
       - Book categories
       - Genre information
         
  - **Junction Tables**
    1. book_authors:
       - Maps books to authors
       - Handles multiple authors per book
    2. book_categories:
       - Maps books to categories
       - Allows multiple categories per book
       
  - **Additional Tables**
    1. industry_identifiers:
       - ISBN and other book identifiers
       - Multiple identifier types per book

## Search Categories
The project collects data across various categories:
1. Technical
   - Python Programming
   - Data Science
   - Machine Learning
   - Web Development
2. Business & Economics
   - Economics
   - Business
3. General Interest
   - Cooking Book
   - English Literature
   - Human Psychology
   - Physics

## Troubleshooting Guide
- Check if MySQL server is running
- Verify connection credentials
- Ensure database exists
- Verify API key is valid
- Monitor API rate limits
- Ensure all libraries are installed
- Check if database has data
    
## Additional Resources
 - Google Books API Documentation
 - Streamlit Documentation
 - MySQL Documentation



