1. 创建控制台应用程序， 版本2.0以上，完整版本，不能为客户端版本
2. 引用： adf.dll, adf.service
3. 修改:  Program为共公public并继承Adf.Service.IService, Main 函数调用 Adf.Service.ServiceHelper.Entry(args);
4. 修改:  若要实现HttpServer，还需为Program继承Adf.Service.IHttpService接口
5. 建立:  app.config 文件
6. 复制 Tool*.bat 并修改Adf.Service.Test为你的应用程序


调试:
项目->属性->调试->命名行参数-> /c
F5 调试

文档：
http://www.aooshi.org/adf/517.html



HA:Node1
HA:Node2
HA:Node3
HA:Keepalive
HA:ElectTimeout
HA:Path
HA:Mails,    ; 
HA:Vip			ip|mask|gateway

