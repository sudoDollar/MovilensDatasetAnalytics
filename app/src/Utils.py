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
    
    def get_age_group(age: int) -> str:

        if 1 <= age <= 10:
            return "6-10"
        elif 11 <= age <= 15:
            return "11-15"
        elif 16 <= age <= 20:
            return "16-20"
        elif 21 <= age <= 25:
            return "21-25"
        elif 26 <= age <= 30:
            return "26-30"
        elif 31 <= age <= 35:
            return "31-35"
        elif 36 <= age <= 40:
            return "36-40"
        elif 41 <= age <= 45:
            return "41-45"
        elif 46 <= age <= 50:
            return "46-50"
        elif 51 <= age <= 55:
            return "51-55"
        elif 56 <= age <= 60:
            return "56-60"
        elif 61 <= age <= 65:
            return "61-65"
        elif 66 <= age <= 70:
            return "66-70"
        else:
            return "80-Beyond"
        
    def get_age_group2(age: int) -> str:

        if 1 <= age <= 10:
            return "1-10"
        elif 11 <= age <= 20:
            return "11-20"
        elif 21 <= age <= 30:
            return "21-30"
        elif 31 <= age <= 40:
            return "31-40"
        elif 41 <= age <= 50:
            return "41-50"
        elif 51 <= age <=60:
            return "51-60"
        else:
            return "60-Beyond"
