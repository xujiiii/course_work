1.worker单独重启命令需要设置 ansible
2.变量设置方法要中心化
3.测试是否有重复结果出现，或是抢占同一id
4.之前出现数据库连接失败，原因未知
5.出现掉线 时，会导致整体的负载上升，可能原因是docker的问题
6.完善celery自动重启
6.1celerypkill时不存在会报错，fixing
7.完善hosts的rabbit，redis，celery，prometheus，flower重启
8.完善posgresql重启
9.写个将authority-key加入全部机器的ansible

test：
均用小于40的测试集进行
0.暂未出现重复运行一个id或结果返回重复
1.单个掉点并重连，会有avgload波动，结果ok
2.全部掉点重连，负载超过4，结果ok    warning   
3.tmux运行 不退出vscode，结果ok
4.tmux运行，推出vscode,有worker掉线(只有一个)，结果在不重新worker登录时正确  16个测试集
5.第二次测试是否会掉线，二次测试没掉线，只要不在ssh连接worker，单纯在hosts上操作就不会掉线，maybe，
 prometheus和flower都在线，数据结果有微小区别