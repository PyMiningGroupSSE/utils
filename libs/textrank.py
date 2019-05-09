import networkx as nx
import numpy as np
import jieba.posseg as pseg
import math

delimiter_list = ['?', '!', ';', '？', '！', '。', '；', '……', '…', '\n']
speech_list = ['an', 'i', 'j', 'l', 'n', 'nr', 'nrfg', 'ns', 'nt', 'nz', 't', 'v', 'vd', 'vn', 'eng']


class TextRank:

    def __init__(self, stopwords_file):
        self.key_sentences = None
        self.stopwords = []
        for word in stopwords_file.readlines():
            self.stopwords.append(word.strip())

    def __segment_text__(self, text):
        sentences = [text]
        for sep in delimiter_list:
            text, sentences = sentences, []
            for seq in text:
                sentences += seq.split(sep)
        sentences = [s.strip() for s in sentences if len(s.strip()) > 0]

        words = []
        for sentence in sentences:
            cutwords = [w.word.strip() for w in pseg.cut(sentence) if w.flag in speech_list]
            cutwords = [word for word in cutwords if len(word) > 0]
            cutwords = [word for word in cutwords if word not in self.stopwords]
            words.append(cutwords)

        return sentences, words

    def sumarize(self, text, pagerank_ratio=0.85):
        self.key_sentences = []
        sentences, words = self.__segment_text__(text)

        matrix_shape = (len(sentences), len(sentences))
        textrank_matrix = np.zeros(shape=matrix_shape)

        for i in range(len(sentences)):
            for j in range(i + 1):
                textrank_matrix[i, j] = textrank_matrix[j, i] = self.__get_sentence_similarity__(words[i], words[j])

        scores = nx.pagerank(nx.from_numpy_matrix(textrank_matrix), pagerank_ratio)
        sorted_scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)

        for index, score in sorted_scores:
            item = dict(index=index, sentence=sentences[index], weight=score)
            self.key_sentences.append(item)

    def get_key_sentences(self, num=6, sentence_min_len=0):
        result = []
        count = 0
        for item in self.key_sentences:
            if count >= num:
                break
            if len(item['sentence']) >= sentence_min_len:
                result.append(item['sentence'])
                count += 1
        return result

    def __get_sentence_similarity__(self, s1_words, s2_words):
        words = list(set(s1_words + s2_words))
        vector1 = [float(s1_words.count(word)) for word in words]
        vector2 = [float(s2_words.count(word)) for word in words]
        vector3 = [vector1[x] * vector2[x] for x in range(len(vector1))]
        vector4 = [1 for num in vector3 if num > 0.]
        co_occur_num = sum(vector4)
        if abs(co_occur_num) <= 1e-12:
            return 0.
        denominator = math.log(float(len(s1_words))) + math.log(float(len(s2_words)))
        if abs(denominator) < 1e-12:
            return 0.
        return co_occur_num / denominator
