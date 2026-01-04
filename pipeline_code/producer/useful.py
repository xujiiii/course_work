

def check_exist(name):
    '''
    如果name已经作为worker上的数据存储地址使用过了
    则会提醒用户更换一个新的name
    或者在worker上删除这个name对应的文件夹
    '''
    