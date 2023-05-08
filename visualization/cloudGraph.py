import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# Load data as pandas dataframe
df = pd.read_csv("C:\\Users\\hairu\\OneDrive\\Desktop\\BigData\\project\\Complete_Codes\\visualization\\words_all.csv", sep='\t')

df = pd.read_csv("C:\\Users\\hairu\\OneDrive\\Desktop\\BigData\\project\\Complete_Codes\\visualization\\words_1999.csv", sep='\t')

df = pd.read_csv("C:\\Users\\hairu\\OneDrive\\Desktop\\BigData\\project\\Complete_Codes\\visualization\\words_2000.csv", sep='\t')

df = pd.read_csv("C:\\Users\\hairu\\OneDrive\\Desktop\\BigData\\project\\Complete_Codes\\visualization\\words_2001.csv", sep='\t')

df = pd.read_csv("C:\\Users\\hairu\\OneDrive\\Desktop\\BigData\\project\\Complete_Codes\\visualization\\words_2002.csv", sep='\t')

# word cloud from frequencies
records = df.to_dict(orient='records')
data = {x["word"]: x["count"] for x in records}
wc = WordCloud(width=1000, height=500, background_color="rgba(255, 255, 255, 0)", mode="RGBA", max_words=1000)
wc.generate_from_frequencies(data)

plt.figure( figsize=(40,20) )
plt.imshow(wc, interpolation="bilinear")
plt.axis("off")
plt.show()
