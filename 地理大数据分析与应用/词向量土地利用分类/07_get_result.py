import pandas as pd

# 读取CSV文件
poi = pd.read_csv('sorted_poi.csv')
land = pd.read_csv('武汉市地块POI.csv')
result = pd.read_csv('clustering_labels.csv')

# 获取face_id列中的唯一值，保持原始顺序
poi_face_id = poi['face_id'].drop_duplicates().tolist()

# 确保poi_face_id的长度与result的行数相同
if len(poi_face_id) != len(result):
    raise ValueError("poi_face_id的长度与result的行数不一致")

# 创建一个包含Id和Clusters列的新DataFrame
clusters_columns = ['Clusters_2', 'Clusters_3', 'Clusters_4', 'Clusters_5']
df_clusters = pd.DataFrame({
    'Id': poi_face_id
})
df_clusters[clusters_columns] = result[clusters_columns].values

# 将Clusters列合并到land表中
land = pd.merge(land, df_clusters, on='Id', how='left')
#
# # 保存更新后的land表
# land.to_csv('updated_land.csv', index=False)

