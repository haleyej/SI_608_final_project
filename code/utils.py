import os
import pandas as pd
import networkx as nx

def load_chunks(directory):
    chunk_dfs = []
    for name in sorted(os.listdir(directory)):
        sub_directory = os.path.join(directory, name)
        if os.path.isdir(sub_directory):
            for filename in sorted(os.listdir(sub_directory)):
                with open(os.path.join(sub_directory, filename), "r", encoding="utf-8") as f:
                    chunk = pd.read_csv(f, encoding = "utf-8")
                    chunk_dfs.append(chunk)
        elif name.endswith(".csv"):
            chunk = pd.read_csv(os.path.join(directory, name), encoding = "utf-8")
            chunk_dfs.append(chunk)

    return pd.concat(chunk_dfs)


def create_network(df_retweets):
    G = nx.DiGraph()
    for _, row in df_retweets.iterrows():
        G.add_edge(row['user_id'], row['retweeted_id'])  # Add an edge from retweeter to the original tweet's author
    return G


def calculate_centrality_and_compare_influence(G, df_users):
    centrality_measures = {
        # centrality metrics, we can define our own
        'degree': nx.degree_centrality(G),
        'betweenness': nx.betweenness_centrality(G),
        'eigenvector': nx.eigenvector_centrality(G, max_iter=1000, tol=1e-06)
    }

    # Convert centrality measures to DataFrame
    centrality_df = pd.DataFrame(centrality_measures)

    # Merge with user data
    df_users_centrality = df_users.join(centrality_df, on='user_id')

    # Sort by different measures to identify key accounts
    for measure in centrality_measures:
        df_users_centrality.sort_values(by=measure, ascending=False, inplace=True)
        print(f"Top users by {measure} centrality:")
        print(df_users_centrality[['user_id', measure, 'follower_count']].head())

    return df_users_centrality