import pandas as pd
import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = 'Songti SC'  # 用来正常显示中文标签
# 1. 读取训练数据
train_data = pd.read_csv('已知.csv')

# 2. 预处理训练数据
# 删除 'OBJECTID' 列
if 'OBJECTID' in train_data.columns:
    train_data = train_data.drop('OBJECTID', axis=1)

# 检查并处理缺失值
if train_data.isnull().values.any():
    # 使用均值填充数值型缺失值
    num_cols = train_data.select_dtypes(include=[np.number]).columns
    train_data[num_cols] = train_data[num_cols].fillna(train_data[num_cols].mean())
    # 使用众数填充分类变量缺失值
    cat_cols = train_data.select_dtypes(exclude=[np.number]).columns
    train_data[cat_cols] = train_data[cat_cols].fillna(train_data[cat_cols].mode().iloc[0])

# 分离特征和目标变量
X_train = train_data.drop('房价', axis=1)
y_train = train_data['房价']

# 处理分类变量（如果有）
X_train = pd.get_dummies(X_train)

# 特征标准化
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)

# 转换为张量
X_train = torch.tensor(X_train, dtype=torch.float32)
y_train = torch.tensor(y_train.values, dtype=torch.float32).view(-1, 1)

# 3. 构建改进的多层感知机模型
class MLP(nn.Module):
    def __init__(self, input_dim):
        super(MLP, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, 32),
            nn.BatchNorm1d(32),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(32, 1)
        )
    def forward(self, x):
        return self.model(x)

input_dim = X_train.shape[1]
model = MLP(input_dim)

# 4. 定义损失函数和优化器，加入学习率调度器
criterion = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=50, gamma=0.1)

# 5. 训练模型
epochs = 200
batch_size = 32

dataset = TensorDataset(X_train, y_train)
loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

loss_history = []

for epoch in range(epochs):
    epoch_loss = 0.0
    for batch_X, batch_y in loader:
        model.train()
        optimizer.zero_grad()
        outputs = model(batch_X)
        loss = criterion(outputs, batch_y)
        loss.backward()
        optimizer.step()
        epoch_loss += loss.item() * batch_X.size(0)
    scheduler.step()
    epoch_loss /= len(loader.dataset)
    loss_history.append(epoch_loss)
    if (epoch+1) % 10 == 0:
        print(f'第 {epoch+1} 轮训练，平均损失值: {epoch_loss:.4f}')

# 对损失值取对数（以自然对数为例）
log_loss_history = np.log(loss_history)

# 绘制对数损失曲线
plt.figure(figsize=(10, 6))
plt.plot(range(1, epochs+1), log_loss_history, label='训练损失（自然对数）')
plt.xlabel('训练轮数')
plt.ylabel('对数损失值')
plt.title('训练损失对数曲线')
plt.legend()

# 保存对数损失曲线图像
plt.savefig('log_loss_curve.png', dpi=600, bbox_inches='tight')

# 显示图像
plt.show()
# # 6. 读取预测数据
# test_data = pd.read_csv('预测.csv')
#
# # 存储 'OBJECTID'（如果存在）
# if 'OBJECTID' in test_data.columns:
#     test_ids = test_data['OBJECTID']
#     test_data = test_data.drop('OBJECTID', axis=1)
# else:
#     test_ids = None
#
# # 检查并处理缺失值
# if test_data.isnull().values.any():
#     # 使用均值填充数值型缺失值
#     num_cols = test_data.select_dtypes(include=[np.number]).columns
#     test_data[num_cols] = test_data[num_cols].fillna(test_data[num_cols].mean())
#     # 使用众数填充分类变量缺失值
#     cat_cols = test_data.select_dtypes(exclude=[np.number]).columns
#     # 检查并处理缺失值
#     if test_data.isnull().values.any():
#         # 使用均值填充数值型缺失值
#         num_cols = test_data.select_dtypes(include=[np.number]).columns
#         test_data[num_cols] = test_data[num_cols].fillna(test_data[num_cols].mean())
#         # 处理分类变量
#         cat_cols = test_data.select_dtypes(exclude=[np.number]).columns
#         if len(cat_cols) > 0:
#             modes = test_data[cat_cols].mode()
#             if len(modes) > 0:
#                 test_data[cat_cols] = test_data[cat_cols].fillna(modes.iloc[0])
#             else:
#                 # 如果无法计算众数，可以选择填充一个默认值，例如字符串 'Unknown'
#                 test_data[cat_cols] = test_data[cat_cols].fillna('Unknown')
#
# # 预处理预测数据
# X_test = test_data
#
# # 确保测试数据的特征与训练数据一致
# X_test = pd.get_dummies(X_test)
# X_test = X_test.reindex(columns=train_data.drop('房价', axis=1).columns, fill_value=0)
#
# # 使用与训练数据相同的标准化器
# X_test = scaler.transform(X_test)
# X_test = torch.tensor(X_test, dtype=torch.float32)
#
# # 7. 进行预测
# model.eval()
# with torch.no_grad():
#     predictions = model(X_test).numpy()
#
# # 8. 保存结果到 'result'
# if test_ids is not None:
#     result = pd.DataFrame({'OBJECTID': test_ids, '预测房价': predictions.flatten()})
# else:
#     result = pd.DataFrame({'预测房价': predictions.flatten()})
#
# result.to_csv('result.csv', index=False, encoding='utf-8-sig')
# # 显示结果
# print(result)