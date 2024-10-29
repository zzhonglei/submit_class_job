from gensim.models import FastText
import jieba
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score

# 读取并处理txt文件
with open("output.txt", "r", encoding="utf-8") as file:
    lines = file.readlines()

# 按行分割，并对每一行用逗号分割成句子，再进行分词处理
paragraphs = [line.strip() for line in lines]
train_data_tokenized = [list(jieba.cut(sentence)) for paragraph in paragraphs for sentence in paragraph.split('，')]

# 训练FastText模型
model = FastText(sentences=train_data_tokenized, vector_size=100, window=7, min_count=1, epochs=100, workers=8)
model.save("fasttext_model_gensim.model")
model = FastText.load("fasttext_model_gensim.model")

# 计算每一段话的向量表示（平均词向量）
paragraph_vectors = []
for paragraph in paragraphs:
    words = []
    for sentence in paragraph.split('，'):
        words.extend(jieba.lcut(sentence))
    word_vectors = []
    for word in words:
        if word in model.wv:
            word_vectors.append(model.wv[word])
    if word_vectors:
        # 计算平均向量
        paragraph_vector = np.mean(word_vectors, axis=0)
    else:
        # 如果段落中没有任何词在词汇表中，用零向量代替
        paragraph_vector = np.zeros(model.vector_size)
    paragraph_vectors.append(paragraph_vector)

# 将段落向量转换为NumPy数组
paragraph_vectors = np.array(paragraph_vectors)

# # 对向量进行L2正则化（归一化）
# normalized_vectors = normalize(paragraph_vectors, norm='l2')
normalized_vectors = paragraph_vectors

# 定义要测试的聚类中心数列表
num_clusters_list = range(2, 11)  # 测试从2到10个聚类中心

# 初始化用于保存指标和聚类结果的列表和数据框
metrics_list = []
labels_df = pd.DataFrame({'Paragraph': paragraphs})

# 对于每个聚类中心数，执行聚类并计算指标
for num_clusters in num_clusters_list:
    # 使用KMeans进行聚类
    kmeans = KMeans(n_clusters=num_clusters, random_state=42)
    kmeans.fit(normalized_vectors)

    # 获取聚类结果
    labels = kmeans.labels_

    # 将聚类标签添加到数据框中
    labels_df[f'Clusters_{num_clusters}'] = labels

    # 计算聚类指标
    silhouette_avg = silhouette_score(normalized_vectors, labels)
    ch_score = calinski_harabasz_score(normalized_vectors, labels)
    db_score = davies_bouldin_score(normalized_vectors, labels)
    inertia = kmeans.inertia_

    # 将指标添加到列表中
    metrics_list.append({
        'Num_Clusters': num_clusters,
        'Silhouette_Score': silhouette_avg,
        'Calinski_Harabasz_Score': ch_score,
        'Davies_Bouldin_Score': db_score,
        'Inertia': inertia
    })

# 将指标列表转换为数据框
metrics_df = pd.DataFrame(metrics_list)

# 将指标保存到文件中
metrics_df.to_csv('clustering_metrics.csv', index=False, encoding='utf-8-sig')

# 将聚类结果保存到文件中
labels_df.to_csv('clustering_labels.csv', index=False, encoding='utf-8-sig')

# 输出聚类指标
print("聚类指标已保存到 'clustering_metrics.csv'")
print(metrics_df)

# 输出聚类结果
print("\n聚类结果已保存到 'clustering_labels.csv'")
print(labels_df)