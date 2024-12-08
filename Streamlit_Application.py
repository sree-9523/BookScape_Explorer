import streamlit as st
import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Set basic style parameters
plt.style.use('default')
sns.set_theme()
plt.rcParams.update({
    'figure.figsize': [10, 6],
    'font.size': 12,
    'font.family': 'sans-serif',
    'axes.grid': True,
    'grid.alpha': 0.3
})


# Database connection function
def init_connection():
    return mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="123456789",
        database="bookscape_explorer"
    )


# Function to run queries
def run_query(query):
    conn = init_connection()
    df = pd.read_sql_query(query, conn)
    return df


# SQL Queries
BOOKS_TABLE = """
    SELECT 
        book_title,
        book_authors,
        categories,
        publication_year,
        averageRating,
        ratingsCount,
        isEbook,
        amount_retailPrice,
        currencyCode_retailPrice
    FROM books
"""

COUNT_BOOKS = """
    SELECT COUNT(*) as total_books FROM books
"""


EBOOK_VS_PHYSICAL = """
    SELECT 
        CASE 
            WHEN isEbook THEN 'eBook'
            ELSE 'Physical Book'
        END as book_type,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM books), 2) as percentage
    FROM books
    GROUP BY isEbook
"""

PUBLISHER_BOOK_COUNT = """
    SELECT 
        p.publisher_name,
        COUNT(*) as book_count
    FROM books b
    JOIN publishers p ON b.publisher_id = p.publisher_id
    GROUP BY p.publisher_id, p.publisher_name
    ORDER BY book_count DESC
    LIMIT 1;
"""

PUBLISHER_RATINGS = """
    SELECT 
        p.publisher_name,
        ROUND(AVG(b.averageRating), 2) as avg_rating,
        COUNT(*) as book_count
    FROM books b
    JOIN publishers p ON b.publisher_id = p.publisher_id
    WHERE b.averageRating IS NOT NULL
    GROUP BY p.publisher_id, p.publisher_name
    HAVING COUNT(*) >= 2  -- Minimum books threshold
    ORDER BY avg_rating DESC
    LIMIT 1;
"""

TOP_EXPENSIVE_BOOKS = """
    SELECT 
        book_title,
        amount_retailPrice,
        currencyCode_retailPrice,
        book_authors
    FROM books
    WHERE amount_retailPrice IS NOT NULL
    ORDER BY amount_retailPrice DESC
    LIMIT 5
"""

PUBLISHED_AFTER_2010 = """
    SELECT 
        book_title,
        publication_year,
        pageCount,
        book_authors
    FROM books
    WHERE publication_year > 2010 
    AND pageCount >= 500
    ORDER BY publication_year, pageCount DESC;
"""

DISCOUNTED_BOOKS = """
    SELECT 
        book_title,
        amount_listPrice,
        amount_retailPrice,
        ROUND(((amount_listPrice - amount_retailPrice) / amount_listPrice * 100), 2) as discount_percentage
    FROM books
    WHERE amount_listPrice IS NOT NULL 
    AND amount_retailPrice IS NOT NULL
    AND amount_listPrice > amount_retailPrice
    AND ((amount_listPrice - amount_retailPrice) / amount_listPrice * 100) > 20
    ORDER BY discount_percentage DESC;
"""

AVERAGE_PAGE_COUNT_EBOOK_VS_PHYSICAL = """
    SELECT 
        CASE 
            WHEN isEbook THEN 'eBook'
            ELSE 'Physical Book'
        END as book_type,
        ROUND(AVG(pageCount), 0) as avg_pages,
        COUNT(*) as book_count
    FROM books
    WHERE pageCount IS NOT NULL
    GROUP BY isEbook;
"""

TOP_AUTHORS = """
    SELECT 
        a.author_name,
        COUNT(DISTINCT ba.book_id) as book_count,
        GROUP_CONCAT(b.book_title) as books
    FROM book_authors ba
    JOIN authors a ON ba.author_id = a.author_id
    JOIN books b ON ba.book_id = b.book_id
    GROUP BY a.author_id, a.author_name  -- Group by both ID and name
    ORDER BY book_count DESC
    LIMIT 3;
"""

PUBLISHER_WITH_MORE_THAN_10_BOOKS = """
    SELECT 
        p.publisher_name,
        COUNT(*) as book_count
    FROM books b
    JOIN publishers p ON b.publisher_id = p.publisher_id
    GROUP BY p.publisher_id, p.publisher_name
    HAVING COUNT(*) > 10
    ORDER BY book_count DESC;
"""

AVERAGE_PAGE_COUNT_PER_CATEGORY = """
    SELECT 
        c.category_name,
        ROUND(AVG(b.pageCount), 0) as avg_pages,
        COUNT(*) as book_count
    FROM books b
    JOIN book_categories bc ON b.book_id = bc.book_id
    JOIN categories c ON bc.category_id = c.category_id
    WHERE b.pageCount IS NOT NULL
    GROUP BY c.category_id, c.category_name
    ORDER BY avg_pages DESC;
"""

BOOKS_WITH_MORE_THAN_3_AUTHORS = """
    SELECT 
        b.book_title,
        COUNT(DISTINCT ba.author_id) as author_count,
        GROUP_CONCAT(a.author_name) as authors
    FROM books b
    JOIN book_authors ba ON b.book_id = ba.book_id
    JOIN authors a ON ba.author_id = a.author_id
    GROUP BY b.book_id, b.book_title
    HAVING COUNT(DISTINCT ba.author_id) > 3;
"""

RATINGS_GREATER_THAN_AVERAGE_RATING = """
    SELECT 
        book_title,
        ratingsCount,
        averageRating
    FROM books
    WHERE ratingsCount > (
        SELECT AVG(ratingsCount)
        FROM books
        WHERE ratingsCount IS NOT NULL
    )
    ORDER BY ratingsCount DESC;
"""

SAME_AUTHOR_PUBLISHED_IN_SAME_YEAR = """
    SELECT 
        a.author_name,
        b1.publication_year,
        COUNT(*) as books_in_year,
        GROUP_CONCAT(b1.book_title) as book_titles
    FROM books b1
    JOIN book_authors ba1 ON b1.book_id = ba1.book_id
    JOIN authors a ON ba1.author_id = a.author_id
    GROUP BY a.author_id, a.author_name, b1.publication_year
    HAVING COUNT(*) > 1
    ORDER BY b1.publication_year DESC, books_in_year DESC;
"""


def search_books_by_keyword(keyword):
    return f"""
        SELECT 
            book_title,
            book_authors,
            publication_year,
            averageRating
        FROM books
        WHERE LOWER(book_title) LIKE LOWER('%{keyword}%')
        ORDER BY COALESCE(averageRating, -1) DESC
    """


YEAR_WITH_HIGHEST_AVERAGE_BOOK_PRICE = """
    SELECT 
        publication_year,
        ROUND(AVG(amount_retailPrice), 2) as avg_price,
        COUNT(*) as book_count
    FROM books
    WHERE publication_year IS NOT NULL 
    AND amount_retailPrice IS NOT NULL
    GROUP BY publication_year
    ORDER BY avg_price DESC
    LIMIT 1;
"""
AUTHORS_PUBLISHED_FOR_3_CONSECUTIVE_YEARS = """
    WITH author_years AS (
        SELECT 
            a.author_id,
            a.author_name,
            b.publication_year,
            COUNT(*) as books_in_year
        FROM authors a
        JOIN book_authors ba ON a.author_id = ba.author_id
        JOIN books b ON ba.book_id = b.book_id
        WHERE b.publication_year IS NOT NULL
        GROUP BY a.author_id, a.author_name, b.publication_year
    ),
    consecutive_years AS (
        SELECT 
            author_id,
            author_name,
            COUNT(*) as consecutive_years
        FROM (
            SELECT 
                author_id,
                author_name,
                publication_year,
                LAG(publication_year, 1) OVER (PARTITION BY author_id ORDER BY publication_year) as prev_year,
                LEAD(publication_year, 1) OVER (PARTITION BY author_id ORDER BY publication_year) as next_year
            FROM author_years
        ) t
        WHERE next_year = publication_year + 1
        AND prev_year = publication_year - 1
        GROUP BY author_id, author_name
    )
    SELECT author_name, consecutive_years
    FROM consecutive_years
    WHERE consecutive_years >= 3;
"""

AUTHORS_PUBLISHED_SAME_YEAR_DIFFERENT_PUBLISHERS = """
    SELECT 
        a.author_name,
        b.publication_year,
        COUNT(DISTINCT b.publisher_id) as publisher_count,
        COUNT(*) as book_count,
        GROUP_CONCAT(DISTINCT p.publisher_name) as publishers
    FROM authors a
    JOIN book_authors ba ON a.author_id = ba.author_id
    JOIN books b ON ba.book_id = b.book_id
    JOIN publishers p ON b.publisher_id = p.publisher_id
    GROUP BY a.author_id, a.author_name, b.publication_year
    HAVING COUNT(DISTINCT b.publisher_id) > 1
    ORDER BY b.publication_year DESC, book_count DESC;
"""

AVERAGE_RETAIL_PRICE_EBOOK_VS_PHYSICAL = """
    SELECT 
        CASE 
            WHEN isEbook THEN 'eBook'
            ELSE 'Physical Book'
        END as book_type,
        ROUND(AVG(amount_retailPrice), 2) as avg_price,
        COUNT(*) as book_count
    FROM books
    WHERE amount_retailPrice IS NOT NULL
    GROUP BY isEbook;
"""

BOOKS_WITH_RATING_OUTLIERS = """
    WITH stats AS (
        SELECT 
            AVG(averageRating) as mean_rating,
            STDDEV(averageRating) as stddev_rating
        FROM books
        WHERE averageRating IS NOT NULL
    )
    SELECT 
        book_title,
        averageRating,
        ratingsCount,
        ROUND((averageRating - mean_rating)/stddev_rating, 2) as z_score
    FROM books, stats
    WHERE averageRating IS NOT NULL
    AND ABS(averageRating - mean_rating) > 2 * stddev_rating
    ORDER BY ABS(averageRating - mean_rating) DESC;
"""

PUBLISHER_WITH_HIGHEST_AVERAGE_RATING = """
    SELECT 
        p.publisher_name,
        ROUND(AVG(b.averageRating), 2) as avg_rating,
        COUNT(*) as book_count
    FROM books b
    JOIN publishers p ON b.publisher_id = p.publisher_id
    WHERE b.averageRating IS NOT NULL
    GROUP BY p.publisher_id, p.publisher_name
    HAVING COUNT(*) > 10
    ORDER BY avg_rating DESC
    LIMIT 1;
"""


def book_distribution_pie_chart(data):
    plt.clf()
    fig, ax = plt.subplots(figsize=(10, 8))
    colors = ['#FF6B6B', '#4ECDC4']

    wedges, texts, autotexts = ax.pie(data['count'],
                                      labels=data['book_type'],
                                      colors=colors,
                                      autopct='%1.1f%%',
                                      startangle=90,
                                      wedgeprops={'width': 0.7, 'edgecolor': 'white'})

    plt.setp(autotexts, size=9, weight="bold")
    plt.setp(texts, size=10)

    ax.set_title('Distribution of Book Types',
                 pad=20,
                 fontsize=14,
                 fontweight='bold')

    ax.legend(data['book_type'],
              title="Book Types",
              loc="center left",
              bbox_to_anchor=(1, 0, 0.5, 1))

    plt.tight_layout()
    return fig


def expense_bar_chart(data):
    plt.clf()
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = sns.color_palette("cubehelix", len(data))

    bars = ax.bar(range(len(data)),
                  data['amount_retailPrice'],
                  color=colors)

    ax.set_title('Top 5 Most Expensive Books',
                 pad=20,
                 fontsize=14,
                 fontweight='bold')
    ax.set_xlabel('Book Titles', fontsize=12)
    ax.set_ylabel(f'Price ({data["currencyCode_retailPrice"].iloc[0]})',
                  fontsize=12)

    ax.set_xticks(range(len(data)))
    ax.set_xticklabels(data['book_title'],
                       rotation=45,
                       ha='right')

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.,
                height,
                f'${height:,.2f}',
                ha='center',
                va='bottom',
                fontsize=10)

    ax.yaxis.grid(True, alpha=0.3)
    ax.xaxis.grid(False)

    plt.tight_layout()
    return fig


def publisher_books_chart(data):
    plt.clf()
    fig, ax = plt.subplots(figsize=(12, 6))

    bars = ax.bar(data['publisher_name'],
                  data['book_count'],
                  color=sns.color_palette("viridis", len(data)))

    ax.set_title('Publishers by Number of Books',
                 pad=20,
                 fontsize=14,
                 fontweight='bold')

    ax.set_xlabel('Publisher', fontsize=12)
    ax.set_ylabel('Number of Books', fontsize=12)

    plt.xticks(rotation=45, ha='right')

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.,
                height,
                f'{int(height)}',
                ha='center',
                va='bottom')

    plt.tight_layout()
    return fig


def publisher_rating_chart(data):
    plt.clf()
    fig, ax = plt.subplots(figsize=(7, 3))

    bars = ax.bar(data['publisher_name'],
                  data['avg_rating'],
                  width=0.2,
                  color=sns.color_palette("viridis", len(data)))

    ax.set_title('Top Publishers by Average Rating',
                 pad=10,
                 fontsize=14,
                 fontweight='bold')

    ax.set_xlabel('Publisher', fontsize=12)
    ax.set_ylabel('Average Rating', fontsize=12)

    plt.xticks(ha='right')

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.,
                height,
                f'{height:.2f}',
                ha='center',
                va='bottom')

    plt.tight_layout()
    return fig


def year_pages_chart(data):
    plt.clf()
    fig, ax = plt.subplots(figsize=(12, 6))

    # Scatter plot
    scatter = ax.scatter(data['publication_year'],
                         data['pageCount'],
                         s=100,  # Point size
                         alpha=0.6,
                         c=data['pageCount'],  # Color based on page count
                         cmap='viridis')

    ax.set_title('Books Published After 2010 with 500+ Pages',
                 pad=20,
                 fontsize=14,
                 fontweight='bold')

    ax.set_xlabel('Publication Year', fontsize=12)
    ax.set_ylabel('Page Count', fontsize=12)

    plt.colorbar(scatter, label='Page Count')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    return fig


def discount_chart(data):
    plt.clf()
    fig, ax = plt.subplots(figsize=(12, 6))

    # Create bar chart for discount percentages
    bars = ax.bar(range(len(data)),
                  data['discount_percentage'],
                  color=sns.color_palette("RdYlGn", len(data)))

    ax.set_title('Books with Discounts > 20%',
                 pad=20,
                 fontsize=14,
                 fontweight='bold')

    ax.set_xlabel('Books', fontsize=12)
    ax.set_ylabel('Discount Percentage (%)', fontsize=12)

    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.,
                height,
                f'{height:.1f}%',
                ha='center',
                va='bottom')

    plt.xticks([])  # Hide x-axis labels as they would be too crowded

    plt.tight_layout()
    return fig


def page_count_comparison(data):
    plt.clf()
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Bar chart
    bars = ax1.bar(data['book_type'],
                   data['avg_pages'],
                   color=['#7E2F8E', '#EDB120'])

    ax1.set_title('Average Page Count Comparison',
                  pad=20,
                  fontsize=14,
                  fontweight='bold')

    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width() / 2.,
                 height,
                 f'{int(height)}',
                 ha='center',
                 va='bottom')

    # Pie chart showing book count distribution
    ax2.pie(data['book_count'],
            labels=data['book_type'],
            autopct='%1.1f%%',
            colors=['#00FFFF', '#0000FF'])

    ax2.set_title('Distribution of Books',
                  pad=20,
                  fontsize=14,
                  fontweight='bold')

    plt.tight_layout()
    return fig


def rating_outliers_chart(data):
    plt.clf()
    fig, ax = plt.subplots(figsize=(7, 3))

    # Scatter plot
    scatter = ax.scatter(data['ratingsCount'],
                         data['averageRating'],
                         c=abs(data['z_score']),
                         cmap='viridis',
                         alpha=0.8,
                         s=80)

    ax.set_title('Rating Outliers Distribution',
                 pad=20,
                 fontsize=12,
                 fontweight='bold')

    ax.set_xlabel('Number of Ratings', fontsize=10)
    ax.set_ylabel('Average Rating', fontsize=10)

    # Add colorbar
    plt.colorbar(scatter, ax=ax, label='|Z-Score|')

    plt.tight_layout()
    return fig


def top_publisher_chart(data):
    plt.clf()
    fig, ax = plt.subplots(figsize=(12, 6))

    # Create bar chart
    bars = ax.bar(data['publisher_name'],
                  data['avg_rating'],
                  alpha=0.8,
                  color=plt.cm.viridis(np.linspace(0, 1, len(data))))

    # Customize chart
    ax.set_title('Top Publishers by Average Rating',
                 pad=20,
                 fontsize=14,
                 fontweight='bold')

    ax.set_xlabel('Publisher')
    ax.set_ylabel('Average Rating')

    # Rotate x-axis labels
    plt.xticks(rotation=45, ha='right')

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2,
                height,
                f'{height:.2f}',
                ha='center',
                va='bottom')

    # Add book count as text below bars
    for i, row in data.iterrows():
        ax.text(i,
                0,
                f'({row["book_count"]} books)',
                ha='center',
                va='top',
                color='darkred')

    plt.tight_layout()
    return fig


def main():
    st.set_page_config(page_title="BookScape Explorer", page_icon="📚", layout="wide")
    st.header("📖 _:orange[Books Data Analysis]_", divider="rainbow")

    try:
        # Default view - Books table
        st.subheader("📚 Books Database")
        books_df = run_query(BOOKS_TABLE)
        total_count = run_query(COUNT_BOOKS).iloc[0]['total_books']
        st.write(f"Total books in database: {total_count}")
        books_df['publication_year'] = books_df['publication_year'].apply(
            lambda x: str(int(x)) if pd.notnull(x) else 'N/A')

        # Add search functionality
        search_term = st.text_input("🔍 Search books by title or author:")
        if search_term:
            filtered_df = books_df[
                books_df['book_title'].str.contains(search_term, case=False, na=False) |
                books_df['book_authors'].str.contains(search_term, case=False, na=False)
                ]
            st.write(f"Found {len(filtered_df)} matching books")
            st.dataframe(filtered_df, use_container_width=True)
        else:
            st.write(f"Showing all {len(books_df)} books")
            st.dataframe(books_df, use_container_width=True)

        # Separator
        st.markdown("---")

        # Query selector
        analysis_option = st.selectbox(
            "📊 Choose Analysis View",
            ["Select an Analysis",
             "eBooks vs Physical Books Distribution",
             "Top 5 Most Expensive Books",
             "Publishers with Most Books",
             "Top Publishers by Rating",
             "Long Books After 2010",
             "Books with Major Discounts",
             "eBook vs Physical Book Page Count",
             "Top Authors Analysis",
             "Publishers with More Than 10 Books",
             "Category Page Count Analysis",
             "Books with Many Authors",
             "Books with Above Average Ratings",
             "Same Author Same Year",
             "Search Books by Keyword",
             "Year with Highest Book Price",
             "Authors Who Published 3 Consecutive Years",
             "Authors in Multiple Publishers",
             "eBook vs Physical Book Prices",
             "Rating Outlier Analysis",
             "Top Publishers by Rating"]
        )

        if analysis_option == "eBooks vs Physical Books Distribution":
            ebook_data = run_query(EBOOK_VS_PHYSICAL)

            # Display data and visualization
            col1, col2 = st.columns([1, 2])

            with col1:
                st.subheader("📋 Distribution Data")
                st.dataframe(ebook_data)

                # Display metrics
                for idx, row in ebook_data.iterrows():
                    st.metric(
                        f"{row['book_type']}s",
                        f"{row['percentage']}%",
                        f"{row['count']} books"
                    )

            with col2:
                st.subheader("📈 Visual Distribution")
                pie_fig = book_distribution_pie_chart(ebook_data)
                st.pyplot(pie_fig)

        elif analysis_option == "Top 5 Most Expensive Books":
            expensive_books = run_query(TOP_EXPENSIVE_BOOKS)

            # Display data and visualization
            st.subheader("📋 Price Data")
            st.dataframe(expensive_books)

            st.subheader("📊 Price Comparison")
            bar_fig = expense_bar_chart(expensive_books)
            st.pyplot(bar_fig)

            # Price statistics
            col1, col2 = st.columns(2)
            with col1:
                st.metric(
                    "Most Expensive Book",
                    f"${expensive_books['amount_retailPrice'].max():,.2f}",
                    delta=expensive_books['book_title'].iloc[0]
                )
            with col2:
                st.metric(
                    "Average Price (Top 5)",
                    f"${expensive_books['amount_retailPrice'].mean():,.2f}",
                    delta="Average of top 5 books"
                )
        elif analysis_option == "Publishers with Most Books":
            publisher_books = run_query(PUBLISHER_BOOK_COUNT)

            st.subheader("📋 Publisher Book Count Data")
            st.dataframe(publisher_books)

            st.subheader("📊 Publisher Book Count Visualization")
            fig = publisher_books_chart(publisher_books)
            st.pyplot(fig)

            col1, col2 = st.columns(2)
            with col1:
                st.metric(
                    "Publisher with Most Books",
                    publisher_books['publisher_name'].iloc[0],
                    f"{publisher_books['book_count'].iloc[0]} books"
                )
            with col2:
                avg_books = publisher_books['book_count'].mean()
                st.metric(
                    "Average Books per Publisher",
                    f"{avg_books:.1f}",
                    f"Top {len(publisher_books)} publishers"
                )

        elif analysis_option == "Top Publishers by Rating":
            publisher_ratings = run_query(PUBLISHER_RATINGS)

            if publisher_ratings.empty:
                st.warning("No publisher ratings data available!")

            else:
                st.subheader("📋 Publisher Ratings Data")
                st.dataframe(publisher_ratings)
                if len(publisher_ratings) > 0:  # Check if we have data
                    st.subheader("📊 Publisher Ratings Visualization")
                    fig = publisher_rating_chart(publisher_ratings)

                    st.pyplot(fig)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric(
                            "Highest Rated Publisher",
                            publisher_ratings['publisher_name'].iloc[0],
                            f"Rating: {publisher_ratings['avg_rating'].iloc[0]}"
                        )

                    with col2:
                        st.metric(
                            "Average Rating Overall",
                            f"{publisher_ratings['avg_rating'].mean():.2f}",
                            f"From {publisher_ratings['book_count'].sum()} books"
                        )

                else:
                    st.warning("Not enough data to create visualization")

        elif analysis_option == "Long Books After 2010":
            long_books = run_query(PUBLISHED_AFTER_2010)

            if not long_books.empty:
                st.subheader("📚 Books Published After 2010 with 500+ Pages")
                st.dataframe(long_books)

                st.subheader("📊 Page Count vs Publication Year")
                fig = year_pages_chart(long_books)
                st.pyplot(fig)

                col1, col2 = st.columns(2)
                with col1:
                    st.metric(
                        "Average Page Count",
                        f"{int(long_books['pageCount'].mean())}",
                        f"{len(long_books)} books"
                    )
                with col2:
                    st.metric(
                        "Longest Book",
                        f"{int(long_books['pageCount'].max())} pages",
                        long_books.loc[long_books['pageCount'].idxmax(), 'book_title']
                    )

        elif analysis_option == "Books with Major Discounts":
            discounted_books = run_query(DISCOUNTED_BOOKS)

            if not discounted_books.empty:
                st.subheader("💰 Books with Discounts Greater than 20%")
                st.dataframe(discounted_books)

                st.subheader("📊 Discount Distribution")
                fig = discount_chart(discounted_books)
                st.pyplot(fig)

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric(
                        "Highest Discount",
                        f"{discounted_books['discount_percentage'].max():.1f}%",
                        discounted_books.loc[discounted_books['discount_percentage'].idxmax(), 'book_title']
                    )
                with col2:
                    st.metric(
                        "Average Discount",
                        f"{discounted_books['discount_percentage'].mean():.1f}%",
                        f"{len(discounted_books)} books"
                    )
                with col3:
                    avg_savings = (discounted_books['amount_listPrice'] - discounted_books['amount_retailPrice']).mean()
                    st.metric(
                        "Average Savings",
                        f"${avg_savings:.2f}",
                        "Per book"
                    )

        elif analysis_option == "eBook vs Physical Book Page Count":
            page_count_data = run_query(AVERAGE_PAGE_COUNT_EBOOK_VS_PHYSICAL)

            if not page_count_data.empty:
                st.subheader("📚 Page Count Comparison")
                st.dataframe(page_count_data)

                st.subheader("📊 Visualization")
                fig = page_count_comparison(page_count_data)
                st.pyplot(fig)

                for _, row in page_count_data.iterrows():
                    st.metric(
                        f"{row['book_type']} Statistics",
                        f"{int(row['avg_pages'])} pages",
                        f"{row['book_count']} books"
                    )

        elif analysis_option == "Top Authors Analysis":
            top_authors = run_query(TOP_AUTHORS)

            if not top_authors.empty:
                st.subheader("📚 Top Authors by Number of Books")
                st.dataframe(top_authors[['author_name', 'book_count']])

        elif analysis_option == "Publishers with More Than 10 Books":
            many_books = run_query(PUBLISHER_WITH_MORE_THAN_10_BOOKS)

            if not many_books.empty:
                st.subheader("📚 Publishers with More Than 10 Books")
                st.dataframe(many_books)
            else:
                st.warning("No data available!")

        elif analysis_option == "Category Page Count Analysis":
            category_pages = run_query(AVERAGE_PAGE_COUNT_PER_CATEGORY)

            if not category_pages.empty:
                st.subheader("📚 Category Analysis")
                st.dataframe(category_pages)

        elif analysis_option == "Books with Many Authors":
            many_authors = run_query(BOOKS_WITH_MORE_THAN_3_AUTHORS)

            if not many_authors.empty:
                st.subheader("📚 Books with More Than 3 Authors")
                st.dataframe(many_authors)

                # Summary statistics
                st.metric(
                    "Number of Books with >3 Authors",
                    len(many_authors),
                    f"Max Authors: {many_authors['author_count'].max()}"
                )
            else:
                st.info("No books found with more than 3 authors.")

        elif analysis_option == "Books with Above Average Ratings":
            above_avg = run_query(RATINGS_GREATER_THAN_AVERAGE_RATING)

            if not above_avg.empty:
                st.subheader("📚 Books with Above Average Number of Ratings")
                st.dataframe(above_avg)

                col1, col2 = st.columns(2)
                with col1:
                    st.metric(
                        "Number of Books",
                        len(above_avg),
                        "Above Average Ratings Count"
                    )
                with col2:
                    st.metric(
                        "Average Rating",
                        f"{above_avg['averageRating'].mean():.2f}",
                        f"From {len(above_avg)} books"
                    )
            else:
                st.info("No books found with above average ratings.")

        elif analysis_option == "Same Author Same Year":
            same_year = run_query(SAME_AUTHOR_PUBLISHED_IN_SAME_YEAR)

            if not same_year.empty:
                st.subheader("📚 Authors with Multiple Books in Same Year")
                st.dataframe(same_year)

                # Summary
                st.metric(
                    "Number of Author-Year Combinations",
                    len(same_year),
                    f"Max Books in a Year: {same_year['books_in_year'].max()}"
                )
            else:
                st.info("No authors found with multiple books in the same year.")

        elif analysis_option == "Search Books by Keyword":
            # Add search input
            search_keyword = st.text_input("Enter keyword to search in book titles:")

            if search_keyword:
                results = run_query(search_books_by_keyword(search_keyword))

                if not results.empty:
                    st.subheader(f"📚 Books containing '{search_keyword}'")
                    st.dataframe(results)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric(
                            "Number of Books Found",
                            len(results),
                            search_keyword
                        )
                    with col2:
                        avg_rating = results['averageRating'].mean()
                        if not pd.isna(avg_rating):
                            st.metric(
                                "Average Rating",
                                f"{avg_rating:.2f}",
                                "of found books"
                            )
                else:
                    st.info(f"No books found containing '{search_keyword}'")

        elif analysis_option == "Year with Highest Book Price":
            year_price = run_query(YEAR_WITH_HIGHEST_AVERAGE_BOOK_PRICE)

            if not year_price.empty:
                st.subheader("📚 Year with Highest Average Book Price")
                st.dataframe(year_price)

                st.metric(
                    f"Highest Average Price Year: {year_price['publication_year'].iloc[0]}",
                    f"${year_price['avg_price'].iloc[0]}",
                    f"{year_price['book_count'].iloc[0]} books"
                )
            else:
                st.info("No price data available.")

        elif analysis_option == "Authors Who Published 3 Consecutive Years":
            consecutive_authors = run_query(AUTHORS_PUBLISHED_FOR_3_CONSECUTIVE_YEARS)

            if not consecutive_authors.empty:
                st.subheader("📚 Authors who Published for 3 Consecutive Years")
                st.dataframe(consecutive_authors[['author_name']])

                st.metric(
                    "Authors with Consecutive Publications",
                    len(consecutive_authors),
                    f"Max Consecutive Years: {consecutive_authors['consecutive_years'].max()}"
                )
            else:
                st.info("No authors found with 3+ consecutive years of publication.")

        elif analysis_option == "Authors in Multiple Publishers":
            multi_publisher = run_query(AUTHORS_PUBLISHED_SAME_YEAR_DIFFERENT_PUBLISHERS)

            if not multi_publisher.empty:
                st.subheader("📚 Authors Published by Multiple Publishers in Same Year")
                st.dataframe(multi_publisher)

                col1, col2 = st.columns(2)
                with col1:
                    st.metric(
                        "Total Authors",
                        len(multi_publisher),
                        "with multiple publishers"
                    )
                with col2:
                    st.metric(
                        "Most Publishers in a Year",
                        multi_publisher['publisher_count'].max(),
                        "for single author"
                    )
            else:
                st.info("No authors found publishing with multiple publishers in the same year.")

        elif analysis_option == "eBook vs Physical Book Prices":
            price_comparison = run_query(AVERAGE_RETAIL_PRICE_EBOOK_VS_PHYSICAL)

            if not price_comparison.empty:
                st.subheader("📚 Average Price Comparison: eBooks vs Physical Books")
                st.dataframe(price_comparison)

                # Display metrics for each book type
                for _, row in price_comparison.iterrows():
                    st.metric(
                        f"{row['book_type']} Statistics",
                        f"${row['avg_price']}",
                        f"{row['book_count']} books"
                    )
            else:
                st.info("No price comparison data available.")

        elif analysis_option == "Rating Outlier Analysis":
            outliers = run_query(BOOKS_WITH_RATING_OUTLIERS)

            if not outliers.empty:
                st.subheader("📚 Books with Unusual Ratings (2+ Standard Deviations)")
                st.dataframe(outliers)

                st.subheader("📊 Rating Outliers Visualization")
                fig = rating_outliers_chart(outliers)
                st.pyplot(fig)

                # Metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    highest_z = outliers.loc[abs(outliers['z_score']).idxmax()]
                    st.metric(
                        "Most Extreme Rating",
                        f"{highest_z['averageRating']:.2f}",
                        f"Z-Score: {highest_z['z_score']:.2f}"
                    )
                with col2:
                    st.metric(
                        "Number of Outliers",
                        len(outliers),
                        "books"
                    )
                with col3:
                    avg_z = abs(outliers['z_score']).mean()
                    st.metric(
                        "Average |Z-Score|",
                        f"{avg_z:.2f}",
                        "standard deviations"
                    )
        elif analysis_option == "Top Publishers by Rating":
            top_publishers = run_query(PUBLISHER_WITH_HIGHEST_AVERAGE_RATING)

            if not top_publishers.empty:
                st.subheader("📚 Top Publishers by Average Rating (>10 books)")
                st.dataframe(top_publishers)

                st.subheader("📊 Publisher Ratings Visualization")
                fig = top_publisher_chart(top_publishers)
                st.pyplot(fig)

                # Metrics
                col1, col2 = st.columns(2)
                with col1:
                    st.metric(
                        "Top Publisher",
                        top_publishers['publisher_name'].iloc[0],
                        f"Rating: {top_publishers['avg_rating'].iloc[0]}"
                    )
                with col2:
                    total_books = top_publishers['book_count'].sum()
                    st.metric(
                        "Total Books Analyzed",
                        total_books,
                        f"Across {len(top_publishers)} publishers"
                    )
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.warning("Please check your database connection and try again.")


if __name__ == "__main__":
    main()
