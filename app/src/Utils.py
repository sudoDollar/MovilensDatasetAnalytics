import pandas as pd

class Utils:
    MOVIE_FILE_PATH = "/Users/ankitrajvanshi/MS CE/Sem 3/Big Data/Project/MovilensDatasetAnalytics/Dataset/movies.dat"
    RATINGS_FILE_PATH = "/Users/ankitrajvanshi/MS CE/Sem 3/Big Data/Project/MovilensDatasetAnalytics/Dataset/ratings.dat"
    USERS_FILE_PATH = "/Users/ankitrajvanshi/MS CE/Sem 3/Big Data/Project/MovilensDatasetAnalytics/Dataset/users.dat"
    file_path = "/Users/ankitrajvanshi/MS CE/Sem 3/Big Data/Project/MovilensDatasetAnalytics/Dataset/zip_code_database.csv"

    def __init__(self):
        dtype_dict = {'zip': str, 'state': str}
        self.df = pd.read_csv(self.file_path, delimiter=',', header=0, dtype=dtype_dict)
        print(self.df.head())

    def get_state(self, zipcode: str) -> str:

        if zipcode in self.df['zip'].values:
            # st = self.df.loc[self.df['zip'] == zipcode, 'state'].iloc[0]
            st = self.df.at[self.df[self.df['zip'] == zipcode].index[0], 'state']
            return st
        else:
            return "None"
        


