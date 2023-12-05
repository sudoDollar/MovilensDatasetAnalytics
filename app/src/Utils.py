import pandas as pd

class Utils:
    MOVIE_FILE_PATH = "Dataset/movies.dat"
    RATINGS_FILE_PATH = "Dataset/ratings.dat"
    USERS_FILE_PATH = "Dataset/users.dat"
    zipcode_file_path = "Dataset/zip_code_database.csv"
    occupation_dict = {
        	"0":  "other",
            "1":  "academic/educator",
            "2":  "artist",
            "3":  "clerical/admin",
            "4":  "college/grad student",
            "5":  "customer service",
            "6":  "doctor/health care",
            "7":  "executive/managerial",
            "8":  "farmer",
            "9":  "homemaker",
            "10":  "K-12 student",
            "11":  "lawyer",
            "12":  "programmer",
            "13":  "retired",
            "14":  "sales/marketing",
            "15":  "scientist",
            "16":  "self-employed",
            "17":  "technician/engineer",
            "18":  "tradesman/craftsman",
            "19":  "unemployed",
            "20":  "writer"
    }

    us_states_dict = {
            'AL': 'Alabama',
            'AK': 'Alaska',
            'AZ': 'Arizona',
            'AR': 'Arkansas',
            'CA': 'California',
            'CO': 'Colorado',
            'CT': 'Connecticut',
            'DE': 'Delaware',
            'FL': 'Florida',
            'GA': 'Georgia',
            'HI': 'Hawaii',
            'ID': 'Idaho',
            'IL': 'Illinois',
            'IN': 'Indiana',
            'IA': 'Iowa',
            'KS': 'Kansas',
            'KY': 'Kentucky',
            'LA': 'Louisiana',
            'ME': 'Maine',
            'MD': 'Maryland',
            'MA': 'Massachusetts',
            'MI': 'Michigan',
            'MN': 'Minnesota',
            'MS': 'Mississippi',
            'MO': 'Missouri',
            'MT': 'Montana',
            'NE': 'Nebraska',
            'NV': 'Nevada',
            'NH': 'New Hampshire',
            'NJ': 'New Jersey',
            'NM': 'New Mexico',
            'NY': 'New York',
            'NC': 'North Carolina',
            'ND': 'North Dakota',
            'OH': 'Ohio',
            'OK': 'Oklahoma',
            'OR': 'Oregon',
            'PA': 'Pennsylvania',
            'RI': 'Rhode Island',
            'SC': 'South Carolina',
            'SD': 'South Dakota',
            'TN': 'Tennessee',
            'TX': 'Texas',
            'UT': 'Utah',
            'VT': 'Vermont',
            'VA': 'Virginia',
            'WA': 'Washington',
            'WV': 'West Virginia',
            'WI': 'Wisconsin',
            'WY': 'Wyoming'
    }
    
    def __init__(self):
        dtype_dict = {'zip': str, 'state': str}
        self.df = pd.read_csv(self.zipcode_file_path, delimiter=',', header=0, dtype=dtype_dict)

    def get_state(self, zipcode: str) -> str:
        if zipcode in self.df['zip'].values:
            st = self.df.at[self.df[self.df['zip'] == zipcode].index[0], 'state']
            return st
        else:
            return "None"
        
    def get_occupation(code: int) -> str:
        return Utils.occupation_dict[str(code)]
    
    def get_state_name(code: str) -> str:
        return Utils.us_states_dict[code]