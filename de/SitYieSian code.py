import happybase
from kafka import KafkaConsumer
import json
import socket
import pandas as pd
import re
import string
import nltk
from nltk.tokenize import word_tokenize

nltk.download('punkt')


english_normalizer = {
    'tmr': 'tomorrow',
    'u': 'you',
    'r': 'are',
    'pls': 'please',
    'plz': 'please',
    'thx': 'thanks',
    'ty': 'thank you',
    'btw': 'by the way',
    'brb': 'be right back',
    'idk': 'i do not know',
    'imho': 'in my humble opinion',
    'lol': 'laugh out loud',
    'omg': 'oh my god',
    'tldr': 'too long did not read',
    'ttyl': 'talk to you later',
    'yw': 'you are welcome',
    'np': 'no problem',
    'w/': 'with',
    'w/o': 'without',
    'b4': 'before',
    'gr8': 'great',
    'msg': 'message',
    'hmu': 'hit me up',
    'l8r': 'later',
    'cya': 'see you',
    'gg': 'good game',
    'min': 'minute',
    'dm': 'direct message',
    'tq': 'thank you',
    'promo' : 'promotion',
    'pm': 'private message',
    'ad': 'already',
    'wan': 'want',
    'd': 'already',
    'servis': 'service',
    'sis': 'sister',
    'cip': 'chip',
    'mby': 'maybe',
    'thru':'through',
    'oult': 'outlet',
    'myb': 'maybe',
    
}

malay_normalizer = {
    'dpt': 'dapat',
    'knp': 'kenapa',
    'sy': 'saya',
    'dh': 'sudah',
    'yg': 'yang',
    'dgn': 'dengan',
    'dlm': 'dalam',
    'org': 'orang',
    'blm': 'belum',
    'jd': 'jadi',
    'bkn': 'bukan',
    'tlg': 'tolong',
    'pstu': 'pastu',
    'skrg': 'sekarang',
    'sm': 'sama',
    'tmpt': 'tempat',
    'bg': 'bagi',
    'trm': 'terima',
    'ksih': 'kasih',
    'smp': 'sampai',
    'msk': 'masuk',
    'hr': 'hari',
    'lm': 'lama',
    'brg': 'barang',
    'pnt': 'penting',
    'mlm': 'malam',
    'nk': 'nak',
    'x': 'tidak',
    'n': 'dan',
    'hahaha': 'tertawa',
    'hm': 'hmm',
    'je': 'sahaja',
    'sgt': 'sangat',
    'tp':'tetapi',
    'ni':'ini',
    'klu':'kalau',
    'kt': 'di',
    'xcm': 'tidak macam',
    'tk': 'tidak',
    'utk': 'untuk',
    'blh': 'boleh',
    'byk': 'banyak',
    'tp': 'tetapi',
    'mmg': 'memang',
    'hb': 'habis',
    'kt': 'di',
    'xcm': 'tidak macam',
    'tk': 'tidak',
    'Sy':'saya',
    'dpt':'dapat',
    'dah':'sudah',
    'ek': 'enak',
    'cm': 'cuma',
    'lg': 'lagi',
    'xsiap': 'tidak siap',
    'xramai': 'tidak ramai',
    'xpnh': 'tidak pernah',
    'xnak': 'tidak mahu',
    'tu': 'itu',
    'tak': 'tidak',
    'pkul': 'pukul',
    'saja': 'sahaja',
    'xde': 'tidak ada',
    'tlong': 'tolong',
    'mkn': 'makan',
    'kat': 'dekat',
    'umh': 'rumah',
    'takpe': 'tidak apa',
    'mcmna': 'macam mana',
    'bli': 'beli',
    'kli': 'kali',
    'xbole': 'tidak boleh',
    'skali': 'sekali',
    'sok':'esok',
    'xcakap': 'tidak cakap',
    'brp': 'berapa',
    'hri': 'hari',
    'dpn':'depan',
    'sbb': 'sebab',
    'trus': 'terus',
    'btul': 'betul',
    'mcm': 'macam',
    'ptg': 'petang',
    'nnt': 'nanti',
    'hnjn': 'hujan',
    'lbt': 'lebat',
    'mcmni':'macam ini',
    'dkt': 'dekat',
    'ltk': 'letak',
    'almt': 'alamat',
    'lngkp': 'lengkap',
    'kalo': 'kalau',
    'jdi': 'jadi',
    'nii': 'ini',
    'jmpa': 'jumpa',
    'rggit': 'ringgit',
    'rm': 'ringgit malaysia',
    'mkan': 'makan',
    'bnda': 'benda',
    'ckp': 'cakap',
    'xnk': 'tidak mahu',
    'nmpk': 'nampak',
}


combined_normalizer = {**english_normalizer, **malay_normalizer}
def preprocess_text(text):
    global normalized_tokens
    # Remove ASCII characters
    text = re.sub(r'[^\x00-\x7F]+', '', text)
    # Remove substrings starting with /u or /U (emoji in Facebook)
    text = re.sub(r'\/[uU]\S+', '', text)
    # Remove punctuations and numbers
    text = re.sub('[^a-zA-Z]', ' ', text)
    # Convert to lowercase
    text = text.lower()
    # Tokenize the text
    tokens = word_tokenize(text)
    # Normalize tokens using the combined normalizer dictionary
    normalized_tokens = [combined_normalizer.get(token, token) for token in tokens]
    # Join the tokens back into a string
    preprocessed_text = ' '.join(normalized_tokens)
    return preprocessed_text

# Connect to HBase
connection = happybase.Connection(port=9090)

table_name = 'domino'
# Check if the table exists
if table_name.encode() in connection.tables():
    # Disable the table before deleting
    connection.disable_table(table_name)
    # Delete the table
    connection.delete_table(table_name)
    print(f"Table {table_name} deleted successfully.")
else:
    print(f"Table {table_name} does not exist.")
    
# Create the table
families = {'data': {}}
connection.create_table(table_name, families)
table = connection.table(table_name)

# In this example, we use your hostname as the Kafka bootstrap server
hostname = socket.gethostname()
bootstrap_servers = f"{hostname}:9092"

# Set up the Kafka consumer
consumer = KafkaConsumer('domino.topic.raw',
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Define function to extract year from date
def extract_year(date):
    pattern = r"\b\d{4}\b"
    year = re.findall(pattern, date)
    return year[0] if year else None

seq = 0
# Continuously poll for new messages
for message in consumer:
    # Extract the JSON data from the Kafka message
    json_data = message.value

    # Preprocess the 'commenterText' column
    commenter_text_clean = preprocess_text(json_data['commenterText'])
    
    # Extract year from 'commentTime' column
    comment_time = extract_year(json_data['commentTime'])

    # Write the data to HBase
    row_key = f"{seq}"
    data = {
        'data:commenterName': str(json_data['commenterName']),
        'data:commenterText': str(commenter_text_clean),
        'data:commentTime': str(comment_time)
    }
    # Convert data to bytes before uploading to HBase
    byte_data = {key.encode(): value.encode() for key, value in data.items()}
    table.put(row_key.encode(), byte_data)
    seq += 1
    
    # Print the current state of the HBase table
    print(f"HBase table '{table_name}': {len(list(table.scan()))} rows")