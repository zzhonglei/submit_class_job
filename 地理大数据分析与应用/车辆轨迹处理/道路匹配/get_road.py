import gotrackit.netreverse.NetGen as ng
import geopandas as gpd


if __name__ == '__main__':
    # 对link.shp的要求: 只需要有geometry字段即可, 但是geometry字段的几何对象必须为LineString类型(不允许Z坐标)
    link_gdf = gpd.read_file(r'/Users/zhonglei/实习任务/大数据实习任务/实习2/data/成都矢量数据/road.shp')
    link_gdf['dir'] = 0  # 根据需要替换 'default_value'
    print(link_gdf)
    # update_link_field_list是需要更新的路网基本属性字段：link_id，from_node，to_node，length，dir
    # 示例中：link_gdf本身已有dir字段，所以没有指定更新dir
    new_link_gdf, new_node_gdf, node_group_status_gdf = ng.NetReverse.create_node_from_link(link_gdf=link_gdf, using_from_to=False,
                                                                                 update_link_field_list=['link_id',
                                                                                                         'from_node',
                                                                                                         'to_node',
                                                                                                         'length'],
                                                                                 plain_crs='EPSG:32648',
                                                                                 modify_minimum_buffer=0.7,
                                                                                 execute_modify=True,
                                                                                 ignore_merge_rule=True,
                                                                                 fill_dir=1,
                                                                                 out_fldr=r'./road/')



























