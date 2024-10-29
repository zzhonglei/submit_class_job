import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans
from gensim.models import FastText

if __name__ == '__main__':
    file_path = "武汉市链接POI.csv"
    df = pd.read_csv(file_path)
    df['训练字段'] = df['大类'] + df['中类']
    poi_type_count = df['训练字段'].nunique()
    print(f"POI的类别共有：{poi_type_count}")
    poi_types = df['训练字段'].unique()
    np.set_printoptions(threshold=6)  # 只显示前后部分，中间部分省略
    print(poi_types)
    # 写入数据
    with open('poi_types.txt', 'w', encoding='utf-8') as f:
        for poi_type in df['训练字段']:
            f.write(poi_type + '\n')

    # 对'训练字段'进行字符级切分
    sentences = []
    for poi_type in df['训练字段']:
        # 将字符串切分为字符列表
        chars = list(poi_type)
        sentences.append(chars)

    # 训练gensim的FastText模型
    model = FastText(sentences=sentences, vector_size=100, window=5, min_count=1, sg=1, epochs=5)

    # 获取句子的向量表示 (对每个poi_type)
    sentence_vectors = []
    for poi_type in poi_types:
        chars = list(poi_type)
        # 获取每个字符的向量，然后取平均
        char_vectors = [model.wv[char] for char in chars if char in model.wv]
        if char_vectors:
            sentence_vector = np.mean(char_vectors, axis=0)
        else:
            sentence_vector = np.zeros(model.vector_size)
        sentence_vectors.append(sentence_vector)

    # 计算余弦相似度矩阵
    similarity_matrix = cosine_similarity(sentence_vectors)
    # 转换为余弦距离矩阵
    distance_matrix = 1 - similarity_matrix
    # 使用 KMeans 进行聚类
    kmeans = KMeans(n_clusters=15, random_state=0)
    kmeans.fit(distance_matrix)
    # 获取聚类标签
    labels = kmeans.labels_
    # 创建一个字典用于存储每个类的句子
    clustered_sentences = {}
    # 根据标签将句子归类
    for sentence, label in zip(poi_types, labels):
        if label not in clustered_sentences:
            clustered_sentences[label] = []
        clustered_sentences[label].append(sentence)

    # 对类别标签进行排序，并按照顺序输出
    for cluster in sorted(clustered_sentences.keys()):
        cluster_sentences = clustered_sentences[cluster]
        sentence_list = "、".join(cluster_sentences)
        # 输出类别编号和数量
        print(f"第 {cluster + 1} 类（共 {len(cluster_sentences)} 个）：{sentence_list}")