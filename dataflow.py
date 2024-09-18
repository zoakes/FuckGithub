import pandas as pd
import glob
import hashlib
import warnings
warnings.filterwarnings('ignore', category=pd.errors.ParserWarning)

def generate_uid(name, length=8):
    """
    Generate a deterministic short hash for a given name.

    Math:

    P(collision) = 1 - e ^ (-n^2) / 2k 
    N = 10,000 (names)
    k = combinations -- 16^d

    targeting < 1% --> p <= .01 
    .01 ~= 1 - e ^ (-10000 ^ 2) / (2 * 16d)

    e piece ~= .99
    16^d≈4.98×10^9
    d≈log_16 (4.98×10^9)
    d ~= 8.06
    """
    # Use MD5 hashing for a consistent, repeatable hash
    hash_object = hashlib.md5(name.encode('utf-8'))
    # Convert to a hex string and take the first 'length' characters
    return hash_object.hexdigest()[:length]



def read_ts_dataflow(pattern= "C:\\Dropbox\\CP Data\\*.txt", encode=True):
    """
    TS seems to write with 4 space delimiter, or 2 space delimiter.
    Fragile, stupid, ugly, and ridiculous -- but functional.
    """
    all_files = glob.glob(pattern)

    name_map = {}
    dfs = []
    for file in all_files:
        file_name = file.split('\\')[-1].split('.txt')[0]
        encode_name = file_name
        if encode:
            # encode_name = str(uuid.uuid4())[:6]
            encode_name = generate_uid(file_name)

        try:
            # Attempt to read the file using whitespace as a delimiter (stupid)
            df = pd.read_csv(file, delim_whitespace=True, header=None, names=['date', 'value'], parse_dates=['date'])
            
            # Check if the 'value' column is numeric; if not, handle it (ugly)
            if not pd.to_numeric(df['value'], errors='coerce').notna().all():
                # Re-read the file with a more flexible delimiter (fragile)
                df = pd.read_csv(file, delimiter=r"\s+", header=None, names=['date', 'value'], engine='python', parse_dates=['date'])
            

            df.rename(columns={'value': encode_name}, inplace=True)
            # ridiculous -- bc it sometimes writes twice
            df.drop_duplicates(inplace=True)
            df.set_index('date', inplace=True)
            dfs.append(df)
            name_map[encode_name] = file_name
            
        except Exception as e:
            print(f"Fucked -- Error reading file {file}: {e}")
            continue

    # outer -- keep dates
    merged_df = pd.concat(dfs, axis=1, join='outer')
    return merged_df, name_map

if __name__ == '__main__':
    incubating, i_map = read_ts_dataflow("C:\\TS_Logs\\*.txt")

    original, p_map = read_ts_dataflow()

    
    incubating.columns = [i_map[i] for i in incubating.columns]
    

    # Maybe store? assert format? safety?
    # read into sql?

    # also -- consider the name map -- we likely want to find a generated name
    # that is long lived -- and then USE that name. not perpetually rewrite over it.
