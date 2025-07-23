import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder , OneHotEncoder
from sqlalchemy import create_engine
pd.set_option('display.max_rows',None)
pd.set_option('display.max_columns', None)
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler

def extract_Music_data(filename:str, output_path:str):
    '''
    Extract sales data from csv file and save it to a new csv file
    @param file_path: str, path to the csv file
    @param output_path: str, path to save the new csv file
    '''
    df_Music = pd.read_csv(filename)
    df_Music.to_parquet(output_path)

def extract_API_Data(filename:str, output_path:str):
    '''
    Transform sales data by imputing missing values and encoding categorical columns
    @param filename: str, path to the csv file
    @param output_path: str, path to save the new csv file
    '''
    df_API = pd.read_csv(filename)
    df_API = df_API[['artist_id', 'main_genre', 'subgenre', 'source']].drop_duplicates(subset=['artist_id'])
    df_API.to_parquet(output_path)

def combine_sources(filename:str, filename1:str, output_path:str):
    df_Music_parquet=pd.read_parquet(filename)
    df_API_parquet=pd.read_parquet(filename1)
    df_Music_combined=pd.merge(df_Music_parquet, df_API_parquet, on='artist_id', how='left')
    df_Music_combined.to_parquet(output_path)

def cleaning(filename:str,output_path:str):
    df_Music=pd.read_parquet(filename)
    df_Music_1=Clean(df_Music)
    df_Music_2=encode_binary_columns(df_Music_1)
    df_Music_2.to_parquet(output_path)


def cleaning_Clustering(filename:str,output_path:str):
    df_Music=pd.read_parquet(filename)
    df_Music_cluster=cluster(df_Music)
    df_Music_cluster.to_parquet(output_path)

def load_to_db(filename:str, table_name:str, postgres_opt:dict):
    '''
    Load the transformed data to the database
    @param filename: str, path to the csv file
    @param table_name: str, name of the table to create
    @param postgres_opt: dict, dictionary containing postgres connection options (user, password, host,port, db)
    '''
    user, password, host, port, db = postgres_opt.values()
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df = pd.read_parquet(filename)
    # Set the index to invoice_id
    df.to_sql(table_name, con=engine, if_exists='replace')


def load_to_db_Clustering(filename:str, table_name:str, postgres_opt:dict):
    '''
    Load the transformed data to the database
    @param filename: str, path to the csv file
    @param table_name: str, name of the table to create
    @param postgres_opt: dict, dictionary containing postgres connection options (user, password, host,port, db)
    '''
    user, password, host, port, db = postgres_opt.values()
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df = pd.read_parquet(filename)
    # Set the index to invoice_id
    df.to_sql(table_name, con=engine, if_exists='replace')
# ---- Helper Functions ----

def Clean(df):
#     make all cols lower case
    df.columns = df.columns.str.lower()
    is_str = df['date'].apply(lambda x: isinstance(x, str))

# Parse only the strings using mixed format
    df.loc[is_str, 'date'] = pd.to_datetime(
    df.loc[is_str, 'date'],
    format='mixed',
    errors='coerce'  # or 'raise' if you want to debug bad values
    )
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['year'] = df['date'].dt.year.astype('Int64')
    df['decade'] = (df['year'] // 10 * 10).astype('Int64')
    scaler = MinMaxScaler()
    tempo_reshaped = df['tempo'].values.reshape(-1, 1)
# Fit and transform the 'tempo' column
    df['tempo_scaled'] = scaler.fit_transform(tempo_reshaped)
    df = df.dropna(subset=['main_genre'])
# Display the first few rows with the new scaled column
    # df.set_index(['customer_id','loan_id'], inplace=True)

    return df


def encode_binary_columns(df):
    # Step 1: Identify binary columns
    binary_cols = [col for col in df.columns if df[col].dropna().nunique() == 2]
    
    # Step 2: Label encode them in-place
    for col in binary_cols:
        unique_vals = df[col].dropna().unique()
        mapping = {val: i for i, val in enumerate(sorted(unique_vals))}
        df[col] = df[col].map(mapping)
    
    # Step 3: Print summary
    print(f"Encoded {len(binary_cols)} binary column(s): {binary_cols}")
    
    return df


def cluster(df):
    df = df.copy()

    # Normalize some subgenres into broader genres
    df.loc[df['subgenre'].str.lower() == 'post-hardcore', 'genres'] = 'metal'
    df.loc[df['subgenre'].str.lower() == 'emo', 'genres'] = 'rock'

    # Define features to use
    selected_features = [
        'danceability', 'energy', 'acousticness', 'instrumentalness',
        'valence', 'tempo', 'speechiness'
    ]

    # Drop rows with missing data
    df_clean = df.dropna(subset=selected_features + ['subgenre', 'genres'])

    # Filter to top 50 subgenres by frequency
    top_subgenres = df_clean['subgenre'].value_counts().nlargest(50).index
    df_clean = df_clean[df_clean['subgenre'].isin(top_subgenres)]

    # Compute mean of selected features per subgenre
    subgenre_avg = df_clean.groupby('subgenre')[selected_features].mean().reset_index()

    # Determine dominant genre for each subgenre
    subgenre_genre_counts = df_clean.groupby(['subgenre', 'genres']).size().reset_index(name='count')
    dominant_genres = subgenre_genre_counts.sort_values('count', ascending=False).drop_duplicates('subgenre')
    subgenre_avg = subgenre_avg.merge(dominant_genres[['subgenre', 'genres']], on='subgenre')
    subgenre_avg.rename(columns={'genres': 'dominant_genre'}, inplace=True)

    # Filter to top 5 dominant genres only
    top_genres = subgenre_avg['dominant_genre'].value_counts().nlargest(5).index
    subgenre_avg = subgenre_avg[subgenre_avg['dominant_genre'].isin(top_genres)]

    # Scale the selected features
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(subgenre_avg[selected_features])

    # Add scaled features back into the DataFrame with suffix
    for i, feature in enumerate(selected_features):
        subgenre_avg[f"{feature}_scaled"] = scaled_features[:, i]

    return subgenre_avg


   

