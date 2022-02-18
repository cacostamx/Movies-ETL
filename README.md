# Movies ETL Process

## Overview

Amazing Prime elaborated an SQL database for a hackaton where teams of analysts will collaborate to work intensively on a project, using the data to solve a problem.  
Data is provided from three different sources: Wikipedia scrape, movies metadata from Kaggle, and ratings data from MovieLens. We used an ETL process to extract the Wikipedia and Kaggle data from their respective files, transform the datasets by cleaning them up and joining them together, and load the cleaned dataset into a SQL database.

Amazing Prime now wants to automate the process of ETL in order to keep the dataset updated on a daily basis.

## Resources

- Data source: The analysis was performed a JSON file and two csv files, which are stored in the /Resources folder.
    - "wikipeda-movies.json"
    - "movies_metadata.csv"
    - "ratings.csv"

- Software use to perform the analysis: Python 3.7.11, Anacoda 4.11, Jupyter Notebook 6.4.6, PostgrSQL v13.5 and Pgadmin4 v6.1 for Windows

## Results

- **Wiki Data**. This data comes in JSON format.  The following general actions were performed.
    - Inspect, consolidate redundant columns, and rename some columns.  The following function was used to consolidate title columns:
         ```python
        def clean_movie(movie):
            movie = dict(movie) #create a non-destructive copy
            alt_titles = {}
            for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                        'Hangul','Hebrew','Hepburn','Japanese','Literally',
                        'Mandarin','McCune–Reischauer','Original title','Polish',
                        'Revised Romanization','Romanized','Russian',
                        'Simplified','Traditional','Yiddish']:
                if key in movie:
                    alt_titles[key] = movie[key]
                    movie.pop(key)
            if len(alt_titles) > 0:
                movie['alt_titles'] = alt_titles
            return movie
        ```
    - Structure data in tabular form as a pandas Data Frane.
    - Parse the IMDB ID from url.
    - Convert data into strings.
    - Parsing dollar data with regular expressions and convert it to numeric.
    - Parsing date and convert it to datetime format.
    - Parsing running time, to extract hours, minutes and seconds and transform to numeric seconds.

- **Kaggle Metadata**. The Kaggle data is much more structured, but it still requires some cleaning:
    - Load data to a pandas Data Frame.
    - Drop data like adult movies.
    - Converting strings to correct data types (numeric and dates).

- **Merge movies datasets**.  After having the movies datasets from Wikipedia and Kaggle, we merged them based on the movie IMDB ID.
    - The merge was an inner join to preserve only the movies that were present on both data sets.
    - We dropped unnecesary columns and filled missing data from both data sets, using a function:
         ```python
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(
                lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
            df.drop(columns=wiki_column, inplace=True)
        ```
    - Rename the columns.

- **Ratings Data**. This data set was also cleaned and transformed:
    - Convert timestamps into datetime format.
    - Group movies with ratings in columns and get a data frame.
    - Merge the ratings data frame with the merged movies data frame, to get a movies with ratings data frame.

- **Create database and export final data frames to SQL tables**.  Since the data from ratings is over 20 million rows, we had to import it in chunks.

    ![export ratings to sql](/Resources/export_sql.png)

    - The final tables in the sql DB are the following:

        |**Ratings table query count**                 |**Movies table query count**                 |
        |:--------------------------------------------:|:-------------------------------------------:|
        |![ratings query](/Resources/ratings_query.png)|![movies query](/Resources/movies_query.png) |
