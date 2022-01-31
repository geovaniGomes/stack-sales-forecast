'''
Font for wordcloud has been downloaded from: https://www.dafont.com/pt/
Function for the viewer is based on that of spaCy: https://github.com/explosion/spacy-streamlit/blob/master/spacy_streamlit/visualizer.py
'''

import streamlit as st
st.set_page_config(layout = 'wide')
import pandas as pd
import numpy as np

import re
from collections import Counter
from collections import OrderedDict
import spacy
from spacy import displacy
import plotly.express as px
import plotly.graph_objects as go

from transformers import pipeline

from wordcloud import WordCloud
import matplotlib.pyplot as plt
from PIL import Image
import numpy as np
from wordcloud import ImageColorGenerator


@st.experimental_memo
def load_dataset():
	path = './data/olist_order_reviews_dataset.csv'
	df = pd.read_csv(path)
	dict_ = {1: 'Strongly negative', 2: 'Weakly negative', 3: 'Neutral', 4: 'Weakly positive', 5: 'Strongly positive'}
	df['review_score'] = df['review_score'].map(dict_)
	df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], format = '%Y-%m-%d')
	df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], format = '%Y-%m-%d')
	return df


def common_words(series):
	series = [i.lower() for i in series.dropna().tolist()]
	bigrams = []
	for doc in nlp.pipe(series, disable = ['attribute_ruler', 'ner']):
		for token in doc:
			if token.pos_ == 'NOUN' or token.pos_ == 'VERB':
				if token.i + 1 < len(doc) and token.i - 1 >= 0:
					if doc[token.i + 1].pos_ == 'ADJ':
						bigrams.append(token.text + ' ' + doc[token.i + 1].text)
	word_freq = Counter(bigrams)
	common = word_freq.most_common(100)
	dict_sorted_by_value = OrderedDict(sorted(dict(common).items(), key = lambda x: x[1], reverse = True))
	return dict_sorted_by_value


@st.experimental_singleton
def load_translator():
	return pipeline('translation', model = 'Helsinki-NLP/opus-mt-mul-en')


@st.experimental_singleton
def load_nlp_hf():
	return pipeline('sentiment-analysis', model = 'arpanghoshal/EmoRoBERTa')


@st.experimental_singleton
def load_nlp():
	return spacy.load('pt_core_news_lg')
	

@st.experimental_singleton
def load_my_nlp():
#	return spacy.load('./model/output/model-best')
	return spacy.load('pt_pipeline_olist')

	
dict_words = {'axo': 'acho', 'blz': 'beleza', 'certinho': 'certo', 'direitinho': 'direito', 'eh': 'é',
              'hj': 'hoje', 'ja': 'já', 'msg': 'mensagem', 'mt': 'muito', 'mto': 'muito', 'nao': 'não', 
              'nf': 'nota fiscal', 'obg': 'obrigado', 'obgd': 'obrigado', 'p': 'para', 'p/': 'para', 
              'pq': 'porque', 'q': 'que', 'qto': 'quanto', 'ta': 'está', 'tá': 'está', 'td': 'todo', 
              'tô': 'estou', 'vc': 'você', 'vcs': 'vocês', 'voces': 'vocês'}

def simple_cleaning(comment):
	if isinstance(comment, str):
		emoji_pattern = re.compile("["
		u"\U0001F600-\U0001F64F"  # emoticons
		u"\U0001F300-\U0001F5FF"  # symbols & pictographs
		u"\U0001F680-\U0001F6FF"  # transport & map symbols
		u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
		"]+", flags = re.UNICODE)
		comment = emoji_pattern.sub(r'', comment)
		comment = re.sub(r'http\S+', '', comment)
		comment = re.sub(r'([!?.,;:]){2,}', r'\1', comment)
		comment = re.sub(r'(\. ){2,}', ' ', comment)
		comment = re.sub(r'k{2,}', ' ', comment)
		comment = re.sub(r'[*_"]', r' ', comment)
		comment = re.sub(r'([a-zA-Z])\s+([;,.?!])', r'\1\2', comment)
		comment = re.sub(r'([;,.:)?!])([a-zA-ZÀ-ÿ])', r'\1 \2', comment)
		comment = re.sub(r'#[a-zA-Z0-9]+', '', comment)
		comment = re.sub(r'[\r\t\n]+', ' ', comment)
		comment = re.sub(r'\s+', ' ', comment)
		comment = re.sub(r'^ ', '', comment)
		comment = re.sub(r' $', '', comment)
		if comment.isupper():
			comment = comment.lower()
		for k, v in dict_words.items():
			comment = re.sub(rf'\b{k}\b', v, comment)      
		sentences = comment.split('. ')
		for index, sentence in enumerate(sentences):
			if len(sentence) > 0:
				sentences[index] = sentence[0].upper() + sentence[1:]
		comment = '. '.join(sentences)          
		if re.match('^[\W]+$', comment):
			comment = np.nan      
	return comment

	
def get_html(html: str):
	"""Convert HTML so it can be rendered."""
	WRAPPER = """<div style="overflow-x: auto; border: 1px solid #e6e9ef; border-radius: 0.25rem; padding: 1rem; margin-bottom: 2.5rem">{}</div>"""
	# Newlines seem to mess with the rendering
	html = html.replace("\n", " ")
	return WRAPPER.format(html)	
	

def visualize_ner(doc, labels, key = None):

	displacy_options = dict()
	displacy_options['colors'] = {'ACTION': '#FFF68F', 'LOC': '#EEE0E5'}

	exp = st.expander('Select entity labels')
	label_select = exp.multiselect('Entity labels', options = labels, default=list(labels),
	key = f'{key}_ner_label_select')

	displacy_options['ents'] = label_select
	html = displacy.render(doc, style = 'ent', options = displacy_options)
	style = '<style>mark.entity { display: it nline-block }</style>'
	st.write(f'{style}{get_html(html)}', unsafe_allow_html=True)


@st.experimental_memo
def plot_1():
	x = df['review_score'].value_counts().index
	y = df['review_score'].value_counts().values
	total = y.sum()
	percents = [round(i*100/total, 2) for i in y]
	fig = go.Figure(data = [go.Bar(x = x, y = y, hovertext = [f'{round(i, 2)}%' for i in percents], width = 0.4)])
	fig.update_traces(marker_color = 'rgb(158,202,225)', marker_line_color = 'rgb(8,48,107)',
		          marker_line_width = 1.5, opacity = 0.6)
	fig.update_layout(title = 'Review Score Count',
		          xaxis = dict(title = 'Review Score'),# tickvals = [1, 2, 3, 4, 5]),
		          yaxis = dict(title = 'Count'),
		          width = 800, height = 500)
	return(fig)
	

@st.experimental_memo	
def plot_2():
	diff_time = df['review_answer_timestamp'] - df['review_creation_date']
	diff_time = diff_time.astype('timedelta64[D]')
	fig = px.box(y = diff_time)
	fig.update_traces(marker_color = 'rgb(158,202,225)', marker_line_color = 'rgb(8,48,107)',
		          marker_line_width = 1.5, opacity = 0.6)
	fig.update_layout(title = 'Time a Review is Given',
		          xaxis = dict(title = 'Review'),
		          yaxis = dict(title = 'Days'),
		          width = 450, height = 500)
	return(fig)
	

@st.experimental_memo	
def plot_3():
	fig = go.Figure(data=[go.Histogram(x = df['review_creation_date'])])
	fig.update_traces(marker_color = 'rgb(158,202,225)', marker_line_color = 'rgb(8,48,107)',
			  opacity = 1)
	fig.update_layout(title = 'Frequency of Reviews over Time',
			  xaxis = dict(title = 'Review Creation Date'),
			  yaxis = dict(title = 'Frequency'),
			  width = 1350, height = 500)
	return(fig)


@st.experimental_memo
def plot_4(score):
	comments = df[(df['review_score'] == score)]['review_comment_message'].apply(simple_cleaning)
	font_path = './wordcloud/Chocolatier Artisanal.ttf'
	mask = np.array(Image.open('./wordcloud/brasil.png'))
	mask_colors = ImageColorGenerator(mask)
	wc = WordCloud(font_path = font_path, mask = mask, background_color = 'white',
	max_words = 100, max_font_size = 256, random_state = 42, width = mask.shape[1],
	height = mask.shape[0], color_func = mask_colors)
	wc.generate_from_frequencies(common_words(comments))
	fig, ax = plt.subplots()
	ax.imshow(wc, interpolation = 'bilinear')
	plt.axis('off')
	return(fig)


df = load_dataset()
translator = load_translator()
nlp = load_nlp()
my_nlp = load_my_nlp()
nlp_hf = load_nlp_hf()
		 
		 
st.title('Reviews')


st.sidebar.title('Data App')
st.sidebar.text('App about reviews made by\nOlist customers')


comment = st.text_area('Enter your review', value = '')
if comment:
	comment = simple_cleaning(comment)
	st.header('Text Categorization 🪄')
	model = st.selectbox(label = 'Select a model', options = ('pt_pipeline_olist', 'EmoRoBERTa'))
	if model == 'pt_pipeline_olist':
		doc = my_nlp(comment)
		st.write(doc.cats)
	else:
		comment_en = translator(comment, src_lang = 'pt')[0]['translation_text']
		st.write(nlp_hf(comment_en)[0])
	st.header('Named Entities Recognition 📝')
	doc = my_nlp(comment)
	visualize_ner(doc, labels = my_nlp.get_pipe('ner').labels, key = 1)		


st.header('Information from the Dataset 📊')
col1, col2 = st.columns([2, 1])
with col1:
	fig = plot_1()
	st.plotly_chart(fig)	
with col2:
	fig = plot_2()
	st.plotly_chart(fig)
fig = plot_3()
st.plotly_chart(fig)


score = st.selectbox(label = 'Select a category', options = ['Strongly negative', 'Weakly negative', 'Neutral', 'Weakly positive', 'Strongly positive'])	
st.dataframe(df[(df['review_score'] == score) & (~df['review_comment_message'].isnull())]['review_comment_message'].sample(5))
fig = plot_4(score)
st.pyplot(fig)
